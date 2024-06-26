import os
import shutil


import sys
sys.path.append("..")

directory = r'C:\kafka\kafka_logs'

for f in os.listdir(directory):
    shutil.rmtree(directory + fr'\{f}')

class StartKafkaCluster():
    def __init__(self, connection, topic_name, current_ip, is_zookeeper, num_brokers):
        self.connection = connection
        self.topic_name = topic_name
        self.current_ip = current_ip
        self.is_zookeeper = is_zookeeper
        self.num_brokers = num_brokers
        
    def start_kafka_cluster(self, df_ips):
        
        print(f"self.is_zookeeper: {self.is_zookeeper}")
        
        if (not self.is_zookeeper) and (self.num_brokers == 1):
            print("Connecting to F1 2021.")
        else:


            with open(r'.\properties\server.properties') as f:
                server_text = f.read()
        

            with open(r'.\properties\zookeeper.properties') as f:
                zookeeper_text = f.read()

            df_ips_zookeeper = df_ips[df_ips['type']=='0']
            df_ips_brokers = df_ips[df_ips['type']=='1']

            default_port = 9092
            listener_port = default_port
            ips = ''

            for i in range(len(df_ips_brokers)):
                broker_id = df_ips_brokers['broker_id'].iloc[i]
                broker_ip = df_ips_brokers['broker_ip'].iloc[i]

                zookeeper_ip = df_ips_zookeeper['broker_ip'].iloc[0]

                listener_port += i

                server_text_new = server_text.replace('{BROKER_ID}', str(broker_id))
                server_text_new = server_text_new.replace('{LISTERNER_IP}', str(broker_ip))
                server_text_new = server_text_new.replace('{ZOOKEEPER_IP}', str(zookeeper_ip))
                server_text_new = server_text_new.replace('{LISTENER_PORT}', str(listener_port))

                print(server_text_new)

                ips += broker_ip + ':' + str(listener_port) + ","

                with open(fr"C:\kafka\config\server{broker_id}.properties", 'w+') as f:
                    f.write(server_text_new)

            with open(fr"C:\kafka\config\zookeeper.properties", 'w+') as f:
                    f.write(zookeeper_text)

            ips = ips[:-1]
            
            try:
                broker_id = df_ips_brokers[df_ips_brokers['broker_ip'] == self.current_ip]['broker_id'].iloc[0]

                if self.is_zookeeper:
                    cmd = rf'"C:\kafka\bin\windows\zookeeper-server-start.bat C:\kafka\config\zookeeper.properties"'
                    print(f'\nStarting zookeeper cmd: \n{cmd}')
                    os.system(f'start cmd.exe /k {cmd}')


                cmd = rf'"C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\server{broker_id}.properties"'
                print(f'\nCreating broker cmd: \n{cmd}')
                os.system(f'start cmd.exe /k {cmd}')

                cmd = rf'"C:\kafka\bin\windows\kafka-topics.bat --create --bootstrap-server {ips} --topic {self.topic_name} --replication-factor 1 --partitions 1"'
                print(f'\nCreating topic cmd: \n{cmd}')
                os.system(f'start cmd.exe /k {cmd}')



                # cmd = rf'"C:\kafka\bin\windows\kafka-console-producer.bat --broker-list {ips} --topic {self.topic_name}"'
                # print(f'\nStarting producer cmd: \n{cmd}')
                # os.system(f'start cmd.exe /k {cmd}')

                # cmd = rf'"C:\kafka\bin\windows\kafka-console-consumer.bat --topic {self.topic_name} --bootstrap-server {ips} --from-beginning"'
                # print(f'\nStarting consumer cmd: \n{cmd}')
                # os.system(f'start cmd.exe /k {cmd}')
            
            except IndexError as e:
                print(e)


