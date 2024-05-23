from login_client import App
from define_kafka_cluster import DefineKafkaCluster



if __name__ == "__main__":
    main = App(app_to_run=DefineKafkaCluster)
    main.mainloop()