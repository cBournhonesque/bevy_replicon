use bevy::{prelude::*, state::app::StatesPlugin};
use bevy_replicon::{prelude::*, test_app::ServerTestAppExt};
use serde::{Deserialize, Serialize};
use test_log::test;

#[test]
fn client_to_server_replication_requires_client_opt_in() {
    let mut server = create_server(ServerPlugin::new(PostUpdate).with_receive_from_clients(true));
    let mut client = create_client(ClientPlugin);
    server.connect_client(&mut client);

    client.world_mut().spawn((Replicated, Marker));

    client.update();
    server.exchange_with_client(&mut client);
    server.update();

    assert_eq!(remote_markers(&mut server), 0);
}

#[test]
fn client_to_server_replication_requires_server_opt_in() {
    let server = create_server(ServerPlugin::new(PostUpdate));

    let channels = server.world().resource::<RepliconChannels>();
    assert_eq!(channels.client_channels().len(), 1);
    assert_eq!(channels.server_channels().len(), 2);
}

#[test]
fn client_to_server_opt_in_replicates_spawned_entity() {
    let mut server = create_server(ServerPlugin::new(PostUpdate).with_receive_from_clients(true));
    let mut client = create_client(ClientPlugin.with_send_to_server(true));
    server.connect_client(&mut client);

    client.world_mut().spawn((Replicated, Marker));

    client.update();
    server.exchange_with_client(&mut client);
    server.update();

    assert_eq!(remote_markers(&mut server), 1);
}

fn create_server(server_plugin: ServerPlugin) -> App {
    let mut app = App::new();
    app.add_plugins((
        MinimalPlugins,
        StatesPlugin,
        RepliconSharedPlugin {
            auth_method: AuthMethod::None,
        },
        server_plugin,
    ))
    .replicate::<Marker>()
    .finish();

    app
}

fn create_client(client_plugin: ClientPlugin) -> App {
    let mut app = App::new();
    app.add_plugins((
        MinimalPlugins,
        StatesPlugin,
        RepliconSharedPlugin {
            auth_method: AuthMethod::None,
        },
        client_plugin,
    ))
    .replicate::<Marker>()
    .finish();

    app
}

fn remote_markers(app: &mut App) -> usize {
    let mut query = app
        .world_mut()
        .query_filtered::<Entity, (With<Remote>, With<Marker>, Without<Replicated>)>();
    query.iter(app.world()).count()
}

#[derive(Component, Deserialize, Serialize)]
struct Marker;
