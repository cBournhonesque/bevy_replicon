use bevy::{prelude::*, state::app::StatesPlugin};
use bevy_replicon::prelude::*;
use test_log::test;

#[test]
fn client_and_server_wrappers_share_reverse_direction_state_in_one_app() {
    let mut app = App::new();
    app.add_plugins((
        MinimalPlugins,
        StatesPlugin,
        RepliconSharedPlugin {
            auth_method: AuthMethod::None,
        },
        ClientPlugin.with_send_to_server(true),
        ServerPlugin::new(PostUpdate).with_receive_from_clients(true),
    ))
    .finish();

    let channels = app.world().resource::<RepliconChannels>();
    assert_eq!(channels.client_channels().len(), 3);
    assert_eq!(channels.server_channels().len(), 3);

    app.world_mut()
        .resource_mut::<NextState<ClientState>>()
        .set(ClientState::Connected);
    app.world_mut()
        .resource_mut::<NextState<ServerState>>()
        .set(ServerState::Running);

    app.update();
    app.update();

    let mut clients = app
        .world_mut()
        .query_filtered::<Entity, With<ConnectedClient>>();
    assert_eq!(clients.iter(app.world()).count(), 1);
}
