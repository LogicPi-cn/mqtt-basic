use log::{error, info, warn};
use std::sync::Arc;
use std::thread;
use std::{process, time::Duration};

extern crate paho_mqtt as mqtt;
pub struct MqttClient {
    client: Arc<mqtt::Client>,
    host: String,
    client_id: String,
    username: String,
    password: String,
    clear: bool,
}

impl MqttClient {
    /// init client using default configuration
    pub fn new(
        host: String,
        client_id: String,
        username: String,
        password: String,
        clear: bool,
    ) -> Self {
        // Define the set of options for the create.
        // Use an ID for a persistent session.
        let create_opts = mqtt::CreateOptionsBuilder::new()
            .server_uri(&host)
            .client_id(&client_id)
            .finalize();

        // Create a client.
        let client = Arc::new(mqtt::Client::new(create_opts).unwrap_or_else(|err| {
            error!("Error creating the client: {:?}", err);
            process::exit(1);
        }));

        MqttClient {
            client,
            host,
            client_id,
            username,
            password,
            clear,
        }
    }

    /// connect to the server
    pub fn connect(&mut self) -> bool {
        // Define the set of options for the connection.
        let conn_opts = mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .user_name(&self.username)
            .password(&self.password)
            .clean_session(self.clear)
            .finalize();

        // Connect and wait for it to complete or fail.
        if let Err(e) = self.client.connect(conn_opts) {
            error!("Unable to connect:\n\t{:?}", e);
            false
        } else {
            info!("Connected to [{}] as [{}]", self.host, self.client_id);
            true
        }
    }

    // Reconnect to the broker when connection is lost.
    pub fn try_reconnect(&mut self) -> bool {
        warn!("Connection lost. Waiting to retry connection");
        for _ in 0..5 {
            thread::sleep(Duration::from_millis(5000));
            if self.client.reconnect().is_ok() {
                info!("Successfully reconnected");
                return true;
            }
        }
        error!("Unable to reconnect after several attempts.");
        false
    }

    /// publis a message to the server
    pub fn publish(&mut self, topic: String, msg: String, qos: i32) -> Result<(), mqtt::Error> {
        info!("Pub [{:?}] <- {:?}", &topic, msg.clone());
        let msg = mqtt::Message::new(topic, msg.clone(), qos);
        let tok = self.client.publish(msg);
        match tok {
            Err(e) => {
                error!("Error sending message: {:?}", e);
                Err(e)
            }
            Ok(_) => Ok(()),
        }
    }

    // disconnect
    pub fn disconnect(&mut self) {
        // If still connected, then disconnect now.
        if self.client.is_connected() {
            println!("Disconnecting");
            self.client.disconnect(None).unwrap();
        }
        println!("Exiting");
    }
}
