pub mod client_ticks;
pub(crate) mod context;
pub mod deferred_entity;
pub(crate) mod mutate_index;
pub(crate) mod receive;
pub mod receive_markers;
pub mod registry;
pub mod rules;
pub(crate) mod send;
pub mod signature;
pub mod track_mutate_messages;
pub mod update_message_flags;

use bevy::prelude::*;

/// Marks an entity for authoritative replication sending.
///
/// Typically inserted on server-owned entities. Received entities are marked
/// with [`Remote`](crate::prelude::Remote) instead.
///
/// See also [`Remote`](crate::prelude::Remote).
#[derive(Component, Default, Reflect, Debug, Clone, Copy)]
#[reflect(Component)]
pub struct Replicated;

/// Provenance for entities materialized from client-to-server replication.
///
/// Inserted on server-side remote entities that were spawned from a connected
/// client and stores the server-side [`ConnectedClient`](crate::prelude::ConnectedClient)
/// entity that originated them.
#[derive(Component, Reflect, Debug, Clone, Copy, PartialEq, Eq)]
#[reflect(Component)]
pub struct ReplicatedFrom(pub Entity);

/// The remote-world entity id that a received [`Remote`](crate::prelude::Remote)
/// entity corresponds to.
///
/// Inserted on receive-side entities for both ordinary server-to-client
/// replication and opt-in client-to-server replication.
#[derive(Component, Reflect, Debug, Clone, Copy, PartialEq, Eq)]
#[reflect(Component)]
pub struct RemoteEntity(pub Entity);
