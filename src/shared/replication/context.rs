use bevy::{
    ecs::entity::{EntityHashMap, hash_set::EntityHashSet},
    prelude::*,
};
use bytes::Bytes;

use crate::{
    client::{ServerUpdateTick, server_mutate_ticks::ServerMutateTicks},
    prelude::*,
    shared::{
        replication::track_mutate_messages::TrackMutateMessages, server_entity_map::ServerEntityMap,
    },
};

/// Explicit receive-side state for one upstream sender.
///
/// In the ordinary client path this is built from the existing singleton
/// resources.
pub(crate) struct ReceiveContext<'a> {
    pub(crate) entity_map: &'a mut ServerEntityMap,
    pub(crate) update_tick: &'a mut ServerUpdateTick,
    pub(crate) buffered_mutations: &'a mut BufferedMutations,
    pub(crate) mutate_ticks: Option<&'a mut ServerMutateTicks>,
    pub(crate) source: Option<Entity>,
    pub(crate) spawned_entities: Option<&'a mut EntityHashSet>,
}

/// Owned receive-side state for a single upstream sender.
#[derive(Default)]
pub(crate) struct ReceiveState {
    entity_map: ServerEntityMap,
    update_tick: ServerUpdateTick,
    buffered_mutations: BufferedMutations,
    mutate_ticks: Option<ServerMutateTicks>,
    spawned_entities: EntityHashSet,
}

impl ReceiveState {
    pub(crate) fn new(track_mutate_messages: TrackMutateMessages) -> Self {
        Self {
            mutate_ticks: (*track_mutate_messages).then(ServerMutateTicks::default),
            ..Default::default()
        }
    }

    pub(crate) fn as_context(&mut self, source: Entity) -> ReceiveContext<'_> {
        ReceiveContext {
            entity_map: &mut self.entity_map,
            update_tick: &mut self.update_tick,
            buffered_mutations: &mut self.buffered_mutations,
            mutate_ticks: self.mutate_ticks.as_mut(),
            source: Some(source),
            spawned_entities: Some(&mut self.spawned_entities),
        }
    }

    #[cfg(test)]
    pub(crate) fn mutate_ticks(&self) -> Option<&ServerMutateTicks> {
        self.mutate_ticks.as_ref()
    }

    pub(crate) fn into_spawned_entities(self) -> EntityHashSet {
        self.spawned_entities
    }
}

/// Receive contexts keyed by the sending client entity on the server.
#[derive(Resource, Default, Deref, DerefMut)]
pub(crate) struct ReceiveContexts(pub(crate) EntityHashMap<ReceiveState>);

/// Builds the current singleton receive context from world resources.
pub(crate) fn with_receive_context<R>(
    world: &mut World,
    f: impl FnOnce(&mut World, &mut ReceiveContext) -> R,
) -> R {
    world.resource_scope(|world, mut entity_map: Mut<ServerEntityMap>| {
        world.resource_scope(|world, mut update_tick: Mut<ServerUpdateTick>| {
            world.resource_scope(|world, mut buffered_mutations: Mut<BufferedMutations>| {
                let mut mutate_ticks = world.remove_resource::<ServerMutateTicks>();
                let mut receive = ReceiveContext {
                    entity_map: &mut entity_map,
                    update_tick: &mut update_tick,
                    buffered_mutations: &mut buffered_mutations,
                    mutate_ticks: mutate_ticks.as_mut(),
                    source: None,
                    spawned_entities: None,
                };
                let result = f(world, &mut receive);
                if let Some(mutate_ticks) = mutate_ticks {
                    world.insert_resource(mutate_ticks);
                }

                result
            })
        })
    })
}

/// Cached buffered mutate messages, used to synchronize mutations with update messages.
#[derive(Resource, Default)]
pub(crate) struct BufferedMutations(pub(crate) Vec<BufferedMutate>);

impl BufferedMutations {
    pub(crate) fn clear(&mut self) {
        self.0.clear();
    }

    pub(crate) fn insert(&mut self, mutation: BufferedMutate) {
        let index = self
            .0
            .partition_point(|other_mutation| mutation.message_tick < other_mutation.message_tick);
        self.0.insert(index, mutation);
    }
}

/// Partially-deserialized mutate message that is waiting for its tick to appear in an update message.
///
/// See also [`crate::server::replication_messages`].
pub(crate) struct BufferedMutate {
    pub(crate) update_tick: RepliconTick,
    pub(crate) message_tick: RepliconTick,
    pub(crate) messages_count: usize,
    pub(crate) message: Bytes,
}
