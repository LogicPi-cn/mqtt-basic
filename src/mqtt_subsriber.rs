extern crate log;
extern crate pretty_env_logger;

use log::{error, info, warn};
use mqtt::Message;
use std::{future::Future, sync::Arc, thread, time::Duration};

use crate::mqtt_publisher::MqttClient;

extern crate paho_mqtt as mqtt;

pub struct MqttSubsriber {
    client: Arc<mqtt::Client>,
    host: String,
    client_id: String,
    topic: String,
    qos: i32,
    username: String,
    password: String,
}

impl MqttSubsriber {
    /// new mqtt server
    pub fn new(
        host: String,
        client_id: String,
        username: String,
        password: String,
        topic: String,
        qos: i32,
    ) -> Self {
        // create options
        let create_opts = mqtt::CreateOptionsBuilder::new()
            .server_uri(&host)
            .client_id(&client_id)
            .finalize();

        // Create a client.
        let cli = Arc::new(mqtt::Client::new(create_opts).unwrap());

        MqttSubsriber {
            client: cli,
            host,
            client_id,
            topic,
            qos,
            username,
            password,
        }
    }

    // connect to server
    pub fn connect(&mut self) -> bool {
        // Define the set of options for the connection.
        let lwt = mqtt::MessageBuilder::new()
            .topic("test")
            .payload("Consumer lost connection")
            .finalize();

        let user_name = &self.username;
        let conn_opts = mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .user_name(user_name)
            .password(&self.password)
            .clean_session(false)
            .will_message(lwt)
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

    // Subscribes single topic
    pub fn subscribe_topic(&mut self) {
        info!("Subscribing topic [{:#?}]", self.topic);
        if let Err(e) = self.client.subscribe(&self.topic, self.qos) {
            error!("Error subscribes topics: {:?}", e);
        } else {
        }
    }

    // Reconnect to the broker when connection is lost.
    pub fn try_reconnect(&mut self) -> bool {
        warn!("Connection lost. Waiting to retry connection");
        for _ in 0..12 {
            thread::sleep(Duration::from_millis(5000));
            if self.client.reconnect().is_ok() {
                info!("Successfully reconnected");
                return true;
            }
        }
        error!("Unable to reconnect after several attempts.");
        false
    }

    // processing rx incoming messages
    pub async fn start<F, Fut>(&mut self, process_fn: F)
    where
        F: Fn(Message) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let rx = self.client.start_consuming();

        info!("Processing requests...");
        for msg in rx.iter() {
            if let Some(msg) = msg {
                process_fn(msg).await;
            } else if !self.client.is_connected() {
                if self.try_reconnect() {
                    info!("Resubscribe topics...");
                    self.subscribe_topic();
                } else {
                    break;
                }
            }
        }
    }

    // disconnect
    pub fn disconnect(&mut self) {
        // If still connected, then disconnect now.
        if self.client.is_connected() {
            println!("Disconnecting");
            self.client.unsubscribe(&self.topic).unwrap();
            self.client.disconnect(None).unwrap();
        }
        println!("Exiting");
    }
}
