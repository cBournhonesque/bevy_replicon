use bevy::{prelude::*, state::app::StatesPlugin};
use bevy_replicon::{
    prelude::*,
    test_app::{ServerTestAppExt, TestClientEntity},
};
use serde::{Deserialize, Serialize};
use test_log::test;

#[test]
fn mixed_direction_server_and_clients_replicate_different_entities() {
    let mut server = create_server(ServerPlugin::new(PostUpdate).with_receive_from_clients(true));
    let mut client_a = create_client(ClientPlugin.with_send_to_server(true));
    let mut client_b = create_client(ClientPlugin.with_send_to_server(true));

    server.connect_client(&mut client_a);
    server.connect_client(&mut client_b);

    let client_a_id = **client_a.world().resource::<TestClientEntity>();
    let client_b_id = **client_b.world().resource::<TestClientEntity>();

    server.world_mut().spawn((Replicated, ServerStateMarker(7)));
    client_a.world_mut().spawn((Replicated, ClientAction(1)));
    client_b.world_mut().spawn((Replicated, ClientAction(2)));

    propagate_mixed_direction(&mut server, [&mut client_a, &mut client_b]);

    let mut server_actions = server
        .world_mut()
        .query_filtered::<(&ClientAction, &ReplicatedFrom), (With<Remote>, Without<Replicated>)>();
    let mut actions = server_actions
        .iter(server.world())
        .map(|(action, source)| (action.0, source.0))
        .collect::<Vec<_>>();
    actions.sort_unstable();
    assert_eq!(actions, vec![(1, client_a_id), (2, client_b_id)]);

    assert_eq!(remote_server_markers(&mut client_a), vec![7]);
    assert_eq!(remote_server_markers(&mut client_b), vec![7]);
    assert!(remote_client_actions(&mut client_a).is_empty());
    assert!(remote_client_actions(&mut client_b).is_empty());
}

#[test]
fn disconnect_cleans_up_only_that_clients_received_entities() {
    let mut server = create_server(ServerPlugin::new(PostUpdate).with_receive_from_clients(true));
    let mut client_a = create_client(ClientPlugin.with_send_to_server(true));
    let mut client_b = create_client(ClientPlugin.with_send_to_server(true));

    server.connect_client(&mut client_a);
    server.connect_client(&mut client_b);

    let client_a_id = **client_a.world().resource::<TestClientEntity>();
    let client_b_id = **client_b.world().resource::<TestClientEntity>();

    client_a.world_mut().spawn((Replicated, ClientAction(1)));
    client_b.world_mut().spawn((Replicated, ClientAction(2)));

    propagate_client_entities(&mut server, [&mut client_a, &mut client_b]);
    assert_eq!(
        remote_client_actions_on_server(&mut server),
        vec![(1, client_a_id), (2, client_b_id)]
    );

    server.disconnect_client(&mut client_a);

    assert_eq!(
        remote_client_actions_on_server(&mut server),
        vec![(2, client_b_id)]
    );
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
    .replicate::<ClientAction>()
    .replicate::<ServerStateMarker>()
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
    .replicate::<ClientAction>()
    .replicate::<ServerStateMarker>()
    .finish();

    app
}

fn propagate_mixed_direction(server: &mut App, mut clients: [&mut App; 2]) {
    for client in &mut clients {
        client.update();
    }
    server.update();
    for client in &mut clients {
        server.exchange_with_client(client);
    }
    server.update();
    for client in &mut clients {
        client.update();
    }
}

fn propagate_client_entities(server: &mut App, mut clients: [&mut App; 2]) {
    for client in &mut clients {
        client.update();
        server.exchange_with_client(client);
    }
    server.update();
}

fn remote_server_markers(app: &mut App) -> Vec<u8> {
    let mut query = app
        .world_mut()
        .query_filtered::<&ServerStateMarker, (With<Remote>, Without<Replicated>)>();
    let mut markers = query
        .iter(app.world())
        .map(|marker| marker.0)
        .collect::<Vec<_>>();
    markers.sort_unstable();
    markers
}

fn remote_client_actions(app: &mut App) -> Vec<u8> {
    let mut query = app
        .world_mut()
        .query_filtered::<&ClientAction, (With<Remote>, Without<Replicated>)>();
    let mut actions = query
        .iter(app.world())
        .map(|action| action.0)
        .collect::<Vec<_>>();
    actions.sort_unstable();
    actions
}

fn remote_client_actions_on_server(app: &mut App) -> Vec<(u8, Entity)> {
    let mut query = app
        .world_mut()
        .query_filtered::<(&ClientAction, &ReplicatedFrom), (With<Remote>, Without<Replicated>)>();
    let mut actions = query
        .iter(app.world())
        .map(|(action, source)| (action.0, source.0))
        .collect::<Vec<_>>();
    actions.sort_unstable();
    actions
}

#[derive(Component, Deserialize, Serialize)]
struct ClientAction(u8);

#[derive(Component, Deserialize, Serialize)]
struct ServerStateMarker(u8);
