pub mod confirm_history;
#[cfg(feature = "client_diagnostics")]
pub mod diagnostics;
pub mod message;
pub mod server_mutate_ticks;

use core::time::Duration;

use bevy::prelude::*;
use log::{Level, debug, error, log_enabled};

use crate::{
    prelude::*,
    shared::{
        replication::{
            context::BufferedMutations, receive::receive_replication, send::enable_send_to_server,
            track_mutate_messages::TrackMutateMessages,
        },
        server_entity_map::ServerEntityMap,
    },
};
use confirm_history::EntityReplicated;
use server_mutate_ticks::{MutateTickReceived, ServerMutateTicks};

/// Client functionality and replication receiving.
///
/// Can be disabled for server-only apps.
pub struct ClientPlugin {
    /// Enables ordinary server-to-client replication receiving.
    pub receive_from_server: bool,

    /// Enables client-to-server replication sending.
    pub send_to_server: bool,

    /// Maximum size for client-authored mutation messages sent to the server.
    pub send_max_size: usize,

    /// The time after which sent mutations will be considered lost if an acknowledgment is not received.
    pub mutations_timeout: Duration,
}

impl ClientPlugin {
    pub const DEFAULT_SEND_MAX_SIZE: usize = 1200;

    pub const fn new() -> Self {
        Self {
            receive_from_server: true,
            send_to_server: false,
            send_max_size: Self::DEFAULT_SEND_MAX_SIZE,
            mutations_timeout: Duration::from_secs(10),
        }
    }

    pub const fn with_receive_from_server(mut self, receive_from_server: bool) -> Self {
        self.receive_from_server = receive_from_server;
        self
    }

    pub const fn with_send_to_server(mut self, send_to_server: bool) -> Self {
        self.send_to_server = send_to_server;
        self
    }

    pub const fn with_send_max_size(mut self, send_max_size: usize) -> Self {
        self.send_max_size = send_max_size;
        self
    }
}

impl Default for ClientPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(non_upper_case_globals)]
pub const ClientPlugin: ClientPlugin = ClientPlugin::new();

impl Plugin for ClientPlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<ClientMessages>()
            .init_resource::<ClientStats>()
            .configure_sets(
                PreUpdate,
                (
                    ClientSystems::ReceivePackets,
                    ClientSystems::Receive,
                    ClientSystems::Diagnostics,
                )
                    .chain(),
            )
            .configure_sets(
                OnEnter(ClientState::Connected),
                (ClientSystems::Receive, ClientSystems::Diagnostics).chain(),
            )
            .configure_sets(
                PostUpdate,
                (ClientSystems::Send, ClientSystems::SendPackets).chain(),
            );

        if self.receive_from_server {
            app.init_resource::<ServerEntityMap>()
                .init_resource::<ServerUpdateTick>()
                .init_resource::<BufferedMutations>()
                .add_message::<EntityReplicated>()
                .add_message::<MutateTickReceived>()
                .add_systems(
                    PreUpdate,
                    receive_replication
                        .in_set(ClientSystems::Receive)
                        .run_if(in_state(ClientState::Connected)),
                )
                .add_systems(
                    OnEnter(ClientState::Connected),
                    receive_replication.in_set(ClientSystems::Receive),
                );
        }

        if self.send_to_server {
            enable_send_to_server(app, self.mutations_timeout, self.send_max_size);
        }

        app.add_systems(
            OnExit(ClientState::Connected),
            reset.in_set(ClientSystems::Reset),
        );

        let auth_method = *app.world().resource::<AuthMethod>();
        debug!("using authorization method `{auth_method:?}`");
        if auth_method == AuthMethod::ProtocolCheck {
            app.add_observer(log_protocol_error).add_systems(
                OnEnter(ClientState::Connected),
                send_protocol_hash.in_set(ClientSystems::SendHash),
            );
        }

        if log_enabled!(Level::Debug) {
            app.add_systems(OnEnter(ClientState::Disconnected), || {
                debug!("disconnected")
            })
            .add_systems(OnEnter(ClientState::Connecting), || debug!("connecting"))
            .add_systems(OnEnter(ClientState::Connected), || debug!("connected"));
        }
    }

    fn finish(&self, app: &mut App) {
        if self.receive_from_server && **app.world().resource::<TrackMutateMessages>() {
            app.init_resource::<ServerMutateTicks>();
        }

        app.world_mut()
            .resource_scope(|world, mut messages: Mut<ClientMessages>| {
                let channels = world.resource::<RepliconChannels>();
                messages.setup_server_channels(channels.server_channels().len());
            });
    }
}

fn reset(
    mut messages: ResMut<ClientMessages>,
    mut stats: ResMut<ClientStats>,
    update_tick: Option<ResMut<ServerUpdateTick>>,
    entity_map: Option<ResMut<ServerEntityMap>>,
    buffered_mutations: Option<ResMut<BufferedMutations>>,
    mutate_ticks: Option<ResMut<ServerMutateTicks>>,
    replication_stats: Option<ResMut<ClientReplicationStats>>,
) {
    messages.clear();
    *stats = Default::default();
    if let Some(mut update_tick) = update_tick {
        *update_tick = Default::default();
    }
    if let Some(mut entity_map) = entity_map {
        entity_map.clear();
    }
    if let Some(mut buffered_mutations) = buffered_mutations {
        buffered_mutations.clear();
    }
    if let Some(mut mutate_ticks) = mutate_ticks {
        mutate_ticks.clear();
    }
    if let Some(mut replication_stats) = replication_stats {
        *replication_stats = Default::default();
    }
}

fn send_protocol_hash(mut commands: Commands, protocol: Res<ProtocolHash>) {
    debug!("sending `{:?}` to the server", *protocol);
    commands.client_trigger(*protocol);
}

fn log_protocol_error(_on: On<ProtocolMismatch>) {
    error!(
        "server reported protocol mismatch; make sure replication rules and events registration order match with the server"
    );
}

/// Set with replication and event systems related to client.
#[derive(SystemSet, Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub enum ClientSystems {
    /// Systems that receive packets from the messaging backend and update [`ClientState`].
    ///
    /// Used by messaging backend implementations.
    ///
    /// Runs in [`PreUpdate`].
    ReceivePackets,
    /// Systems that read data from [`ClientMessages`].
    ///
    /// Runs in [`PreUpdate`] and [`OnEnter`] for [`ClientState::Connected`] (to avoid 1 frame delay).
    Receive,
    /// Systems that populate Bevy's [`Diagnostics`](bevy::diagnostic::Diagnostics).
    ///
    /// Runs in [`PreUpdate`] and [`OnEnter`] for [`ClientState::Connected`] (to avoid 1 frame delay).
    Diagnostics,
    /// System that sends [`ProtocolHash`].
    ///
    /// Runs in [`OnEnter`] for [`ClientState::Connected`].
    SendHash,
    /// Systems that write data to [`ClientMessages`].
    ///
    /// Runs in [`PostUpdate`].
    Send,
    /// Systems that send packets to the messaging backend.
    ///
    /// Used by messaging backend implementations.
    ///
    /// Runs in [`PostUpdate`].
    SendPackets,
    /// Systems that reset the client.
    ///
    /// Runs in [`OnExit`] for [`ClientState::Connected`].
    Reset,
}

/// Last received tick for update messages from the server.
///
/// In other words, the last [`RepliconTick`] with a removal, insertion, spawn or despawn.
/// This value is not updated when mutation messages are received from the server.
///
/// See also [`ServerMutateTicks`].
#[derive(Resource, Deref, Default, Reflect, Debug, Clone, Copy)]
pub struct ServerUpdateTick(pub(crate) RepliconTick);

/// Replication stats during message processing.
///
/// Statistic will be collected only if the resource is present.
/// The resource is not added by default.
///
/// See also [`ClientDiagnosticsPlugin`]
/// for automatic integration with Bevy diagnostics.
#[derive(Resource, Default, Reflect, Debug, Clone, Copy)]
pub struct ClientReplicationStats {
    /// Incremented per entity that changes.
    pub entities_changed: usize,
    /// Incremented for every component that changes.
    pub components_changed: usize,
    /// Incremented per client mapping added.
    pub mappings: usize,
    /// Incremented per entity despawn.
    pub despawns: usize,
    /// Replication messages received.
    pub messages: usize,
    /// Replication bytes received in message payloads (without internal messaging plugin data).
    pub bytes: usize,
}

/// Marker for entities spawned by replication.
///
/// Automatically inserted for each newly received entity.
///
/// See also [`Replicated`].
#[derive(Component, Default, Reflect, Debug, Clone, Copy)]
#[reflect(Component)]
pub struct Remote;
