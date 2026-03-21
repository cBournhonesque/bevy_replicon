use bevy::{ecs::entity::EntityHashMap, prelude::*};
use bytes::{Buf, Bytes};
use log::{debug, error, trace};
use postcard::experimental::max_size::MaxSize;

use crate::{
    client::{
        ClientReplicationStats, Remote, ServerUpdateTick,
        confirm_history::{ConfirmHistory, EntityReplicated},
        server_mutate_ticks::MutateTickReceived,
    },
    postcard_utils,
    prelude::*,
    shared::{
        backend::{
            channels::{ClientChannel, ClientToServerReplicationChannels, ServerChannel},
            server_messages::ServerMessages,
        },
        replication::{
            context::{
                BufferedMutate, ReceiveContext, ReceiveContexts, ReceiveState, with_receive_context,
            },
            deferred_entity::{DeferredChanges, DeferredEntity},
            mutate_index::MutateIndex,
            receive_markers::{EntityMarkers, ReceiveMarkers},
            registry::{
                ReplicationRegistry,
                ctx::{DespawnCtx, EntitySpawner, RemoveCtx, WriteCtx},
            },
            signature::SignatureMap,
            track_mutate_messages::TrackMutateMessages,
            update_message_flags::UpdateMessageFlags,
        },
        server_entity_map::{EntityEntry, ServerEntityMap},
    },
};

/// Enables internal server-side receiving for client-to-server replication.
///
/// This is intentionally crate-private until the public opt-in API lands.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn enable_receive_from_clients(app: &mut App) -> ClientToServerReplicationChannels {
    let channels = {
        let mut channels = app.world_mut().resource_mut::<RepliconChannels>();
        channels.create_client_to_server_replication_channels()
    };

    app.world_mut().insert_resource(channels);
    app.world_mut().init_resource::<ReceiveContexts>();
    app.world_mut()
        .resource_scope(|world, mut messages: Mut<ServerMessages>| {
            let channels = world.resource::<RepliconChannels>();
            messages.setup_client_channels(channels.client_channels().len());
        });

    let track_mutate_messages = *app.world().resource::<TrackMutateMessages>();
    let existing_clients = app
        .world_mut()
        .query_filtered::<Entity, With<ConnectedClient>>()
        .iter(app.world())
        .collect::<Vec<_>>();
    let mut contexts = app.world_mut().resource_mut::<ReceiveContexts>();
    for client in existing_clients {
        contexts
            .entry(client)
            .or_insert_with(|| ReceiveState::new(track_mutate_messages));
    }

    channels
}

/// Receives and applies replication messages from the server.
///
/// Update messages are sent over the [`ServerChannel::Updates`] and are applied first to ensure valid state
/// for component mutations.
///
/// Mutate messages are sent over [`ServerChannel::Mutations`], which means they may appear
/// ahead-of or behind update messages from the same server tick. A mutation will only be applied if its
/// update tick has already appeared in an update message, otherwise it will be buffered while waiting.
/// Since component mutations can arrive in any order, they will only be applied if they correspond to a more
/// recent server tick than the last acked server tick for each entity.
///
/// Buffered mutate messages are processed last.
///
/// Acknowledgments for received mutate messages are sent back to the server.
///
/// See also [`ReplicationMessages`](crate::server::replication_messages::ReplicationMessages).
pub(crate) fn receive_replication(
    world: &mut World,
    mut changes: Local<DeferredChanges>,
    mut entity_markers: Local<EntityMarkers>,
    mut received_messages: Local<ReceivedReplicationMessages>,
) {
    world.resource_scope(|world, mut messages: Mut<ClientMessages>| {
        messages.drain_received_into(ServerChannel::Updates, &mut received_messages.updates);
        messages.drain_received_into(ServerChannel::Mutations, &mut received_messages.mutations);

        with_receive_params(world, &mut changes, &mut entity_markers, |world, params| {
            with_receive_context(world, |world, receive| {
                if let Some(acks) =
                    apply_replication(world, receive, params, &mut received_messages)
                {
                    messages.send(ClientChannel::MutationAcks, acks);
                }
            });
        });
    })
}

/// Receives and applies replication messages from multiple clients into per-client receive contexts.
pub(crate) fn receive_replication_from_clients(
    world: &mut World,
    mut changes: Local<DeferredChanges>,
    mut entity_markers: Local<EntityMarkers>,
    mut peer_messages: Local<PeerReceivedMessages>,
) {
    let Some(channels) = world
        .get_resource::<ClientToServerReplicationChannels>()
        .copied()
    else {
        return;
    };
    let track_mutate_messages = *world.resource::<TrackMutateMessages>();

    world.resource_scope(|world, mut messages: Mut<ServerMessages>| {
        messages.drain_received_into(channels.updates, &mut peer_messages.updates);
        messages.drain_received_into(channels.mutations, &mut peer_messages.mutations);
        peer_messages.prepare();
        if peer_messages.is_empty() {
            return;
        }

        with_receive_params(world, &mut changes, &mut entity_markers, |world, params| {
            world.resource_scope(|world, mut contexts: Mut<ReceiveContexts>| {
                for (client, received_messages) in peer_messages.iter_mut() {
                    let receive_state = contexts
                        .entry(client)
                        .or_insert_with(|| ReceiveState::new(track_mutate_messages));

                    let mut receive = receive_state.as_context();
                    if let Some(acks) =
                        apply_replication(world, &mut receive, params, received_messages)
                    {
                        messages.send(client, channels.mutation_acks, acks);
                    }

                    received_messages.clear();
                }

                peer_messages.retain(|client| contexts.contains_key(&client));
            });
        });
    });
}

pub(crate) fn add_receive_context(
    add: On<Add, ConnectedClient>,
    contexts: Option<ResMut<ReceiveContexts>>,
    track_mutate_messages: Res<TrackMutateMessages>,
) {
    let Some(mut contexts) = contexts else {
        return;
    };

    contexts.insert(add.entity, ReceiveState::new(*track_mutate_messages));
}

pub(crate) fn remove_receive_context(
    remove: On<Remove, ConnectedClient>,
    contexts: Option<ResMut<ReceiveContexts>>,
) {
    let Some(mut contexts) = contexts else {
        return;
    };

    contexts.remove(&remove.entity);
}

pub(crate) fn sync_receive_contexts(
    mut contexts: ResMut<ReceiveContexts>,
    clients: Query<Entity, With<ConnectedClient>>,
    track_mutate_messages: Res<TrackMutateMessages>,
) {
    for client in &clients {
        contexts
            .entry(client)
            .or_insert_with(|| ReceiveState::new(*track_mutate_messages));
    }

    contexts.retain(|client, _| clients.get(*client).is_ok());
}

/// Reads all received messages and applies them.
///
/// Sends acknowledgments for mutate messages back.
fn apply_replication(
    world: &mut World,
    receive: &mut ReceiveContext,
    params: &mut ReceiveParams,
    messages: &mut ReceivedReplicationMessages,
) -> Option<Vec<u8>> {
    for mut message in messages.updates.drain(..) {
        if let Err(e) = apply_update_message(world, receive, params, &mut message) {
            error!("unable to apply update message: {e}");
        }
    }

    // Unlike update messages, we read all mutate messages first, sort them by tick
    // in descending order to ensure that the last mutation will be applied first.
    // Since mutate messages manually split by packet size, we apply all messages,
    // but skip outdated data per-entity by checking last received tick for it
    // (unless user requested history via marker).
    let update_tick = *receive.update_tick;
    if !messages.mutations.is_empty() {
        let acks_size = MutateIndex::POSTCARD_MAX_SIZE * messages.mutations.len();
        let mut acks = Vec::with_capacity(acks_size);
        for message in messages.mutations.drain(..) {
            if let Err(e) = buffer_mutate_message(receive, params, message, &mut acks) {
                error!("unable to buffer mutate message: {e}");
            }
        }
        apply_mutate_messages(world, receive, params, update_tick);
        Some(acks)
    } else {
        apply_mutate_messages(world, receive, params, update_tick);
        None
    }
}

/// Reads and applies an update message.
///
/// For details see [`replication_messages`](crate::server::replication_messages).
fn apply_update_message(
    world: &mut World,
    receive: &mut ReceiveContext,
    params: &mut ReceiveParams,
    message: &mut Bytes,
) -> Result<()> {
    if let Some(stats) = &mut params.stats {
        stats.messages += 1;
        stats.bytes += message.len();
    }

    let flags: UpdateMessageFlags = postcard_utils::from_buf(message)?;
    debug_assert!(!flags.is_empty(), "message can't be empty");

    let message_tick = postcard_utils::from_buf(message)?;
    trace!("applying update message with `{flags:?}` for {message_tick:?}");
    receive.update_tick.0 = message_tick;

    let last_flag = flags.last();
    for (_, flag) in flags.iter_names() {
        let array_kind = if flag != last_flag {
            ArrayKind::Sized
        } else {
            ArrayKind::Dynamic
        };

        match flag {
            UpdateMessageFlags::MAPPINGS => {
                let len = apply_array(array_kind, message, |message| {
                    apply_entity_mapping(world, receive, params, message)
                })
                .map_err(|e| format!("unable to apply mappings: {e}"))?;
                if let Some(stats) = &mut params.stats {
                    stats.mappings += len;
                }
            }
            UpdateMessageFlags::DESPAWNS => {
                let len = apply_array(array_kind, message, |message| {
                    apply_despawn(world, receive, params, message, message_tick)
                })
                .map_err(|e| format!("unable to apply despawns: {e}"))?;
                if let Some(stats) = &mut params.stats {
                    stats.despawns += len;
                }
            }
            UpdateMessageFlags::REMOVALS => {
                let len = apply_array(array_kind, message, |message| {
                    apply_removals(world, receive, params, message, message_tick)
                })
                .map_err(|e| format!("unable to apply removals: {e}"))?;
                if let Some(stats) = &mut params.stats {
                    stats.entities_changed += len;
                }
            }
            UpdateMessageFlags::CHANGES => {
                debug_assert_eq!(array_kind, ArrayKind::Dynamic);
                let len = apply_array(array_kind, message, |message| {
                    apply_changes(world, receive, params, message, message_tick)
                })
                .map_err(|e| format!("unable to apply changes: {e}"))?;
                if let Some(stats) = &mut params.stats {
                    stats.entities_changed += len;
                }
            }
            _ => unreachable!("iteration should yield only named flags"),
        }
    }

    Ok(())
}

/// Reads and buffers mutate message.
///
/// For details see [`replication_messages`](crate::server::replication_messages).
///
/// Returns mutate index to be used for acknowledgment.
fn buffer_mutate_message(
    receive: &mut ReceiveContext,
    params: &mut ReceiveParams,
    mut message: Bytes,
    acks: &mut Vec<u8>,
) -> Result<()> {
    if let Some(stats) = &mut params.stats {
        stats.messages += 1;
        stats.bytes += message.len();
    }

    let update_tick = postcard_utils::from_buf(&mut message)?;
    let message_tick = postcard_utils::from_buf(&mut message)?;
    let messages_count = if receive.mutate_ticks.is_some() {
        postcard_utils::from_buf(&mut message)?
    } else {
        1
    };
    let mutate_index: MutateIndex = postcard_utils::from_buf(&mut message)?;
    trace!("received mutate message for {message_tick:?} requiring update tick {update_tick:?}");
    receive.buffered_mutations.insert(BufferedMutate {
        update_tick,
        message_tick,
        messages_count,
        message,
    });

    postcard_utils::to_extend_mut(&mutate_index, acks)?;

    Ok(())
}

/// Applies mutations from [`BufferedMutations`].
///
/// If the mutate message can't be applied yet (because the update message with the
/// corresponding tick hasn't arrived), it will be kept in the buffer.
fn apply_mutate_messages(
    world: &mut World,
    receive: &mut ReceiveContext,
    params: &mut ReceiveParams,
    update_tick: ServerUpdateTick,
) {
    let entity_map = &mut *receive.entity_map;
    let mutate_ticks = &mut receive.mutate_ticks;
    receive.buffered_mutations.0.retain_mut(|mutate| {
        if mutate.update_tick > *update_tick {
            return true;
        }

        trace!("applying mutate message for {:?}", mutate.message_tick);
        let len = apply_array(ArrayKind::Dynamic, &mut mutate.message, |message| {
            apply_mutations(world, entity_map, params, message, mutate.message_tick)
        });

        match len {
            Ok(len) => {
                if let Some(stats) = &mut params.stats {
                    stats.entities_changed += len;
                }
            }
            Err(e) => error!(
                "unable to apply mutate message for tick `{:?}`: {e}",
                mutate.message_tick
            ),
        }

        if let Some(mutate_ticks) = mutate_ticks.as_deref_mut()
            && mutate_ticks.confirm(mutate.message_tick, mutate.messages_count)
            && let Some(mutate_tick_received) = params.mutate_tick_received.as_deref_mut()
        {
            mutate_tick_received.write(MutateTickReceived {
                tick: mutate.message_tick,
            });
        }

        false
    });
}

/// Deserializes and applies the mapping from a server entity to a client
/// entity by comparing hashes calculated from the [`Signature`] component.
fn apply_entity_mapping(
    world: &mut World,
    receive: &mut ReceiveContext,
    params: &mut ReceiveParams,
    message: &mut Bytes,
) -> Result<()> {
    let server_entity = postcard_utils::entity_from_buf(message)?;
    let hash = u64::from_le_bytes(postcard_utils::from_buf(message)?);

    let Some(client_entity) = params.signature_map.get(hash) else {
        debug!(
            "skipping unknown hash 0x{hash:016x} for `{server_entity}` (client entity may have been despawned already)"
        );
        return Ok(());
    };

    debug!("mapping `{server_entity}` to `{client_entity}` using hash 0x{hash:016x}");
    receive.entity_map.insert(server_entity, client_entity);
    world.entity_mut(client_entity).insert(Remote);

    Ok(())
}

/// Deserializes and applies entity despawn from update message.
fn apply_despawn(
    world: &mut World,
    receive: &mut ReceiveContext,
    params: &mut ReceiveParams,
    message: &mut Bytes,
    message_tick: RepliconTick,
) -> Result<()> {
    let server_entity = postcard_utils::entity_from_buf(message)?;
    if let Some(client_entity) = receive.entity_map.server_entry(server_entity).remove() {
        params.signature_map.remove(client_entity);
        if let Ok(client_entity) = world.get_entity_mut(client_entity) {
            trace!("applying despawn for `{}`", client_entity.id());
            let ctx = DespawnCtx { message_tick };
            (params.registry.despawn)(&ctx, client_entity);
        }
    }

    Ok(())
}

/// Deserializes and applies component removals for an entity.
fn apply_removals(
    world: &mut World,
    receive: &mut ReceiveContext,
    params: &mut ReceiveParams,
    message: &mut Bytes,
    message_tick: RepliconTick,
) -> Result<()> {
    let server_entity = postcard_utils::entity_from_buf(message)?;
    let data_size: usize = postcard_utils::from_buf(message)?;

    let client_entity = *receive
        .entity_map
        .to_client()
        .get(&server_entity)
        .ok_or_else(|| format!("received removal for unknown server's `{server_entity}`"))?;

    let Ok(mut client_entity) = world
        .get_entity_mut(client_entity)
        .map(|entity| DeferredEntity::new(entity, params.changes))
    else {
        debug!("ignoring removals for despawned `{client_entity}`");
        message.advance(data_size);
        return Ok(());
    };

    params
        .entity_markers
        .read(params.receive_markers, &*client_entity);

    confirm_tick(
        &mut client_entity,
        params.replicated.as_deref_mut(),
        message_tick,
    );

    let mut data = message.split_to(data_size);
    let len = apply_array(ArrayKind::Dynamic, &mut data, |data| {
        let fns_id = postcard_utils::from_buf(data)?;
        let (_, component_id, fns) = params.registry.get(fns_id);
        let mut ctx = RemoveCtx {
            message_tick,
            component_id,
        };
        trace!(
            "applying removal for `{}` with `{fns_id:?}`",
            client_entity.id()
        );

        fns.remove(&mut ctx, params.entity_markers, &mut client_entity);

        Ok(())
    })?;

    if let Some(stats) = &mut params.stats {
        stats.components_changed += len;
    }

    client_entity.flush();

    Ok(())
}

/// Deserializes and applies component insertions and/or mutations for an entity.
fn apply_changes(
    world: &mut World,
    receive: &mut ReceiveContext,
    params: &mut ReceiveParams,
    message: &mut Bytes,
    message_tick: RepliconTick,
) -> Result<()> {
    let server_entity = postcard_utils::entity_from_buf(message)?;
    let data_size: usize = postcard_utils::from_buf(message)?;

    let world_cell = world.as_unsafe_world_cell();
    let mut spawner = EntitySpawner::new(unsafe { world_cell.world_mut() });
    let world = unsafe { world_cell.world_mut() };

    let mut client_entity = match receive.entity_map.server_entry(server_entity) {
        EntityEntry::Occupied(entry) => {
            let Ok(client_entity) = world.get_entity_mut(entry.get()) else {
                debug!("ignoring changes for despawned `{}`", entry.get());
                message.advance(data_size);
                return Ok(());
            };

            DeferredEntity::new(client_entity, params.changes)
        }
        EntityEntry::Vacant(entry) => {
            let mut client_entity = DeferredEntity::new(world.spawn_empty(), params.changes);
            client_entity.insert(Remote);
            entry.insert(client_entity.id());
            client_entity
        }
    };

    params
        .entity_markers
        .read(params.receive_markers, &*client_entity);

    confirm_tick(
        &mut client_entity,
        params.replicated.as_deref_mut(),
        message_tick,
    );

    let mut data = message.split_to(data_size);
    let len = apply_array(ArrayKind::Dynamic, &mut data, |data| {
        let fns_id = postcard_utils::from_buf(data)?;
        let (_, component_id, fns) = params.registry.get(fns_id);
        let mut ctx = WriteCtx {
            entity_map: receive.entity_map,
            type_registry: params.type_registry,
            component_id,
            message_tick,
            spawner: &mut spawner,
            ignore_mapping: false,
        };
        trace!(
            "applying change for `{}` with `{fns_id:?}`",
            client_entity.id(),
        );

        fns.write(&mut ctx, params.entity_markers, &mut client_entity, data)?;

        Ok(())
    })?;

    if let Some(stats) = &mut params.stats {
        stats.components_changed += len;
    }

    client_entity.flush();

    Ok(())
}

fn apply_array(
    kind: ArrayKind,
    message: &mut Bytes,
    mut f: impl FnMut(&mut Bytes) -> Result<()>,
) -> Result<usize> {
    match kind {
        ArrayKind::Sized => {
            let len = postcard_utils::from_buf(message)?;
            for _ in 0..len {
                (f)(message)?;
            }

            Ok(len)
        }
        ArrayKind::Dynamic => {
            let mut len = 0;
            while message.has_remaining() {
                (f)(message)?;
                len += 1;
            }

            Ok(len)
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
enum ArrayKind {
    Sized,
    Dynamic,
}

fn confirm_tick(
    entity: &mut DeferredEntity,
    replicated: Option<&mut Messages<EntityReplicated>>,
    tick: RepliconTick,
) {
    if let Some(mut history) = entity.get_mut::<ConfirmHistory>() {
        history.set_last_tick(tick);
    } else {
        entity.insert(ConfirmHistory::new(tick));
    }
    if let Some(replicated) = replicated {
        replicated.write(EntityReplicated {
            entity: entity.id(),
            tick,
        });
    }
}

/// Deserializes and applies component mutations for an entity.
fn apply_mutations(
    world: &mut World,
    entity_map: &mut ServerEntityMap,
    params: &mut ReceiveParams,
    message: &mut Bytes,
    message_tick: RepliconTick,
) -> Result<()> {
    let server_entity = postcard_utils::entity_from_buf(message)?;
    let data_size: usize = postcard_utils::from_buf(message)?;

    let Some(&client_entity) = entity_map.to_client().get(&server_entity) else {
        debug!("ignoring mutations received for unknown server's `{server_entity}`");
        message.advance(data_size);
        return Ok(());
    };

    let world_cell = world.as_unsafe_world_cell();
    let mut spawner = EntitySpawner::new(unsafe { world_cell.world_mut() });
    let world = unsafe { world_cell.world_mut() };

    let Ok(mut client_entity) = world
        .get_entity_mut(client_entity)
        .map(|entity| DeferredEntity::new(entity, params.changes))
    else {
        debug!("ignoring mutations for despawned `{client_entity}`");
        message.advance(data_size);
        return Ok(());
    };

    params
        .entity_markers
        .read(params.receive_markers, &*client_entity);

    let Some(mut history) = client_entity.get_mut::<ConfirmHistory>() else {
        return Err(format!(
            "`{}` missing history component inserted on the first update message",
            client_entity.id()
        )
        .into());
    };

    let new_tick = message_tick > history.last_tick();
    if new_tick {
        history.set_last_tick(message_tick);
    } else {
        if !params.entity_markers.need_history() {
            trace!("ignoring outdated mutations for `{}`", client_entity.id());
            message.advance(data_size);
            return Ok(());
        }

        let ago = history.last_tick().get().wrapping_sub(message_tick.get());
        if ago >= u64::BITS {
            trace!(
                "discarding {ago} ticks old mutations for `{}`",
                client_entity.id()
            );
            message.advance(data_size);
            return Ok(());
        }

        history.set(ago);
    }
    if let Some(replicated) = params.replicated.as_deref_mut() {
        replicated.write(EntityReplicated {
            entity: client_entity.id(),
            tick: message_tick,
        });
    }

    let mut data = message.split_to(data_size);
    let len = apply_array(ArrayKind::Dynamic, &mut data, |data| {
        let fns_id = postcard_utils::from_buf(data)?;
        let (_, component_id, fns) = params.registry.get(fns_id);
        let mut ctx = WriteCtx {
            entity_map,
            type_registry: params.type_registry,
            component_id,
            message_tick,
            spawner: &mut spawner,
            ignore_mapping: false,
        };
        trace!(
            "applying mutation for `{}` with `{fns_id:?}`",
            client_entity.id(),
        );

        if new_tick {
            fns.write(&mut ctx, params.entity_markers, &mut client_entity, data)?;
        } else {
            fns.consume_or_write(
                &mut ctx,
                params.entity_markers,
                params.receive_markers,
                &mut client_entity,
                data,
            )?;
        }

        Ok(())
    })?;

    if let Some(stats) = &mut params.stats {
        stats.components_changed += len;
    }

    client_entity.flush();

    Ok(())
}

fn with_receive_params<R>(
    world: &mut World,
    changes: &mut DeferredChanges,
    entity_markers: &mut EntityMarkers,
    f: impl FnOnce(&mut World, &mut ReceiveParams) -> R,
) -> R {
    world.resource_scope(|world, mut signature_map: Mut<SignatureMap>| {
        world.resource_scope(|world, receive_markers: Mut<ReceiveMarkers>| {
            world.resource_scope(|world, registry: Mut<ReplicationRegistry>| {
                let type_registry = world.resource::<AppTypeRegistry>().clone();
                let mut stats = world.remove_resource::<ClientReplicationStats>();
                let mut replicated = world.remove_resource::<Messages<EntityReplicated>>();
                let mut mutate_tick_received =
                    world.remove_resource::<Messages<MutateTickReceived>>();

                let mut params = ReceiveParams {
                    changes,
                    entity_markers,
                    signature_map: &mut signature_map,
                    replicated: replicated.as_mut(),
                    mutate_tick_received: mutate_tick_received.as_mut(),
                    stats: stats.as_mut(),
                    receive_markers: &receive_markers,
                    registry: &registry,
                    type_registry: &type_registry,
                };
                let result = f(world, &mut params);

                if let Some(stats) = stats {
                    world.insert_resource(stats);
                }
                if let Some(replicated) = replicated {
                    world.insert_resource(replicated);
                }
                if let Some(mutate_tick_received) = mutate_tick_received {
                    world.insert_resource(mutate_tick_received);
                }

                result
            })
        })
    })
}

#[derive(Default)]
pub(crate) struct ReceivedReplicationMessages {
    updates: Vec<Bytes>,
    mutations: Vec<Bytes>,
}

impl ReceivedReplicationMessages {
    fn clear(&mut self) {
        self.updates.clear();
        self.mutations.clear();
    }

    fn is_empty(&self) -> bool {
        self.updates.is_empty() && self.mutations.is_empty()
    }
}

#[derive(Default)]
pub(crate) struct PeerReceivedMessages {
    updates: Vec<(Entity, Bytes)>,
    mutations: Vec<(Entity, Bytes)>,
    peers: EntityHashMap<ReceivedReplicationMessages>,
}

impl PeerReceivedMessages {
    fn is_empty(&self) -> bool {
        self.updates.is_empty()
            && self.mutations.is_empty()
            && self
                .peers
                .values()
                .all(ReceivedReplicationMessages::is_empty)
    }

    fn iter_mut(&mut self) -> impl Iterator<Item = (Entity, &mut ReceivedReplicationMessages)> {
        self.peers
            .iter_mut()
            .map(|(&entity, messages)| (entity, messages))
    }

    fn retain(&mut self, mut f: impl FnMut(Entity) -> bool) {
        self.peers.retain(|entity, _| f(*entity));
    }

    fn prepare(&mut self) {
        for messages in self.peers.values_mut() {
            messages.clear();
        }

        for (client, message) in self.updates.drain(..) {
            self.peers.entry(client).or_default().updates.push(message);
        }
        for (client, message) in self.mutations.drain(..) {
            self.peers
                .entry(client)
                .or_default()
                .mutations
                .push(message);
        }
    }
}

/// Borrowed resources from the world and locals.
///
/// To avoid passing a lot of arguments into all receive functions.
struct ReceiveParams<'a> {
    changes: &'a mut DeferredChanges,
    entity_markers: &'a mut EntityMarkers,
    signature_map: &'a mut SignatureMap,
    replicated: Option<&'a mut Messages<EntityReplicated>>,
    mutate_tick_received: Option<&'a mut Messages<MutateTickReceived>>,
    stats: Option<&'a mut ClientReplicationStats>,
    receive_markers: &'a ReceiveMarkers,
    registry: &'a ReplicationRegistry,
    type_registry: &'a AppTypeRegistry,
}

#[cfg(test)]
mod tests {
    use bevy::state::app::StatesPlugin;
    use serde::{Deserialize, Serialize};
    use test_log::test;

    use super::*;
    use crate::{
        server::server_tick::ServerTick, shared::replication::track_mutate_messages::TrackAppExt,
    };

    #[test]
    fn two_senders_distinct_entities() {
        let (mut receiver, channels) = create_receiver(false);
        let mut sender_a = create_sender(false);
        let mut sender_b = create_sender(false);

        let [connection_a, connection_b] =
            connect_senders(&mut receiver, [&mut sender_a, &mut sender_b]);

        let entity_a = sender_a.world_mut().spawn((Replicated, Marker(1))).id();
        let entity_b = sender_b.world_mut().spawn((Replicated, Marker(2))).id();
        assert_eq!(
            entity_a, entity_b,
            "both senders should use the same raw entity ID to exercise map collisions"
        );

        sender_a.update();
        sender_b.update();
        exchange_sender_messages(&mut sender_a, &mut receiver, &connection_a, channels);
        exchange_sender_messages(&mut sender_b, &mut receiver, &connection_b, channels);
        receiver.update();
        exchange_receiver_acks(&mut receiver, &mut sender_a, &connection_a, channels);
        exchange_receiver_acks(&mut receiver, &mut sender_b, &connection_b, channels);
        sender_a.update();
        sender_b.update();

        let mut remotes = receiver
            .world_mut()
            .query_filtered::<&Marker, (With<Remote>, Without<Replicated>)>();
        let mut values = remotes
            .iter(receiver.world())
            .map(|marker| marker.0)
            .collect::<Vec<_>>();
        values.sort_unstable();
        assert_eq!(values, vec![1, 2]);
    }

    #[test]
    fn two_senders_same_tick_mutations() {
        let (mut receiver, channels) = create_receiver(true);
        let mut sender_a = create_sender(true);
        let mut sender_b = create_sender(true);

        let [connection_a, connection_b] =
            connect_senders(&mut receiver, [&mut sender_a, &mut sender_b]);

        let entity_a = sender_a
            .world_mut()
            .spawn((Replicated, BoolComponent(false)))
            .id();
        let entity_b = sender_b
            .world_mut()
            .spawn((Replicated, BoolComponent(false)))
            .id();
        assert_eq!(
            entity_a, entity_b,
            "both senders should use the same raw entity ID to exercise map collisions"
        );

        sender_a.update();
        sender_b.update();
        exchange_sender_messages(&mut sender_a, &mut receiver, &connection_a, channels);
        exchange_sender_messages(&mut sender_b, &mut receiver, &connection_b, channels);
        receiver.update();
        exchange_receiver_acks(&mut receiver, &mut sender_a, &connection_a, channels);
        exchange_receiver_acks(&mut receiver, &mut sender_b, &connection_b, channels);
        sender_a.update();
        sender_b.update();

        sender_a
            .world_mut()
            .get_mut::<BoolComponent>(entity_a)
            .unwrap()
            .0 = true;
        sender_b
            .world_mut()
            .get_mut::<BoolComponent>(entity_b)
            .unwrap()
            .0 = true;

        sender_a.update();
        sender_b.update();
        let tick_a = **sender_a.world().resource::<ServerTick>();
        let tick_b = **sender_b.world().resource::<ServerTick>();
        assert_eq!(tick_a, tick_b, "mutation ticks should align across senders");

        exchange_sender_messages(&mut sender_a, &mut receiver, &connection_a, channels);
        exchange_sender_messages(&mut sender_b, &mut receiver, &connection_b, channels);
        receiver.update();
        exchange_receiver_acks(&mut receiver, &mut sender_a, &connection_a, channels);
        exchange_receiver_acks(&mut receiver, &mut sender_b, &connection_b, channels);
        sender_a.update();
        sender_b.update();

        let mut remotes = receiver
            .world_mut()
            .query_filtered::<&BoolComponent, (With<Remote>, Without<Replicated>)>();
        assert_eq!(
            remotes
                .iter(receiver.world())
                .filter(|component| component.0)
                .count(),
            2
        );

        let contexts = receiver.world().resource::<ReceiveContexts>();
        assert_eq!(contexts.len(), 2);
        for receive_state in contexts.values() {
            let mutate_ticks = receive_state
                .mutate_ticks()
                .expect("mutate tracking should be enabled per sender");
            assert!(
                mutate_ticks.contains(tick_a),
                "each sender should track its own mutation confirmations"
            );
        }
    }

    fn create_receiver(track_mutate_messages: bool) -> (App, ClientToServerReplicationChannels) {
        let mut app = App::new();
        app.add_plugins((
            MinimalPlugins,
            StatesPlugin,
            RepliconSharedPlugin {
                auth_method: AuthMethod::None,
            },
            ServerPlugin::new(PostUpdate),
            ServerMessagePlugin,
        ))
        .replicate::<Marker>()
        .replicate::<BoolComponent>();
        if track_mutate_messages {
            app.track_mutate_messages();
        }
        app.finish();

        let channels = enable_receive_from_clients(&mut app);

        (app, channels)
    }

    fn create_sender(track_mutate_messages: bool) -> App {
        let mut app = App::new();
        app.add_plugins((
            MinimalPlugins,
            StatesPlugin,
            RepliconSharedPlugin {
                auth_method: AuthMethod::None,
            },
            ServerPlugin::new(PostUpdate),
            ServerMessagePlugin,
        ))
        .replicate::<Marker>()
        .replicate::<BoolComponent>();
        if track_mutate_messages {
            app.track_mutate_messages();
        }
        app.finish();

        app
    }

    fn connect_senders<const N: usize>(
        receiver: &mut App,
        mut senders: [&mut App; N],
    ) -> [SenderConnection; N] {
        receiver
            .world_mut()
            .resource_mut::<NextState<ServerState>>()
            .set(ServerState::Running);
        for sender in &mut senders {
            sender
                .world_mut()
                .resource_mut::<NextState<ServerState>>()
                .set(ServerState::Running);
        }

        let connections = core::array::from_fn(|index| {
            let receiver_on_sender = senders[index]
                .world_mut()
                .spawn(ConnectedClient { max_size: 1200 })
                .id();
            let sender_on_receiver = receiver
                .world_mut()
                .spawn(ConnectedClient { max_size: 1200 })
                .id();

            SenderConnection {
                receiver_on_sender,
                sender_on_receiver,
            }
        });

        for sender in &mut senders {
            sender.update();
        }
        receiver.update();
        for sender in &mut senders {
            sender.update();
        }
        receiver.update();

        for sender in &mut senders {
            sender.world_mut().resource_mut::<ServerMessages>().clear();
        }
        receiver
            .world_mut()
            .resource_mut::<ServerMessages>()
            .clear();

        connections
    }

    fn exchange_sender_messages(
        sender: &mut App,
        receiver: &mut App,
        connection: &SenderConnection,
        channels: ClientToServerReplicationChannels,
    ) {
        let messages = sender
            .world_mut()
            .resource_mut::<ServerMessages>()
            .drain_sent()
            .collect::<Vec<_>>();
        let mut receiver_messages = receiver.world_mut().resource_mut::<ServerMessages>();

        for (client, channel_id, message) in messages {
            assert_eq!(client, connection.receiver_on_sender);
            let reverse_channel = match channel_id {
                id if id == usize::from(ServerChannel::Updates) => channels.updates,
                id if id == usize::from(ServerChannel::Mutations) => channels.mutations,
                other => panic!("unexpected sender channel {other}"),
            };
            receiver_messages.insert_received(
                connection.sender_on_receiver,
                reverse_channel,
                message,
            );
        }
    }

    fn exchange_receiver_acks(
        receiver: &mut App,
        sender: &mut App,
        connection: &SenderConnection,
        channels: ClientToServerReplicationChannels,
    ) {
        let mut sender_messages = sender.world_mut().resource_mut::<ServerMessages>();
        receiver
            .world_mut()
            .resource_mut::<ServerMessages>()
            .retain_sent(|(client, channel_id, message)| {
                if *client == connection.sender_on_receiver {
                    if *channel_id == channels.mutation_acks {
                        sender_messages.insert_received(
                            connection.receiver_on_sender,
                            ClientChannel::MutationAcks,
                            message.clone(),
                        );
                    }
                    false
                } else {
                    true
                }
            });
    }

    #[derive(Component, Deserialize, Serialize)]
    struct Marker(u8);

    #[derive(Component, Deserialize, Serialize)]
    struct BoolComponent(bool);

    #[derive(Clone, Copy, PartialEq, Eq)]
    struct SenderConnection {
        receiver_on_sender: Entity,
        sender_on_receiver: Entity,
    }
}
