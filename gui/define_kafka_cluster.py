import sys
sys.path.append("..")

from typing import Tuple
import customtkinter as ctk
import tkinter as tk
from tkinter import ttk
from customtkinter import CTkButton, CTkCheckBox, CTkFrame, \
    CTkEntry, BooleanVar, CTkToplevel, CTkLabel, CENTER, CTkImage
    
from PIL import Image

from threading import Thread
import time


import socket
import pandas as pd
import sqlite3

from ctkspinbox import Spinbox
from enter_ips import IPsWindow

from rcp_kafka.kafka_producer import RcpKafkaProducer
from threading import Thread

from utils import create_df_ips

from rcp_kafka.start_kafka_cluster import StartKafkaCluster


# This file creates a customtkinter window which allows the user to configure the Kafka cluster.
# It creates a connection to a SQLite database, which will contain the values of the IP addresses
# for the zookeeper and borkers.

# Create a connection to the ips_db database in the root folder, and create the ips table.
connection = sqlite3.connect(r'..\ips_db.db')
connection.execute('''CREATE TABLE IF NOT EXISTS ips 
    (
        type INTEGER,
        broker_id INTEGER,
        broker_ip TEXT
    )'''
)
connection.commit()

# The CTkTopLevel class is one which creates a new window in the same application.
class DefineKafkaCluster(CTkToplevel):
    def __init__(self, master, topic_name, access_token):
        super().__init__(master)
        
        self.title("Start Kafka Cluster")
        
        self.topic_name = topic_name
        self.geom_size = "440x460"
        self.geometry(self.geom_size)
        self.resizable(0, 0)
        self.access_token = access_token
        
        print(self.access_token)
        
        self.widgets_configure_server()
        
        # Initialize the variables for the check boxes to check if this machine is the zookeeper / broker 0.
        self.is_zookeeper = BooleanVar(value=True)
        self.is_broker_0 = BooleanVar(value=True)
        self.can_close = False
        
        # If the current machine is the zookeeper, use the current IP address
        self.current_ip = socket.gethostbyname(socket.gethostname())
        
        # Initialize the zookeeper and broker_0 IPs
        self.zookeeper_ip = None
        self.broker_0_ip = None
        
        # Create a frame for the widgets that will allow the user to configure the Kafka cluster.
        self.frm_custom_ips = CTkFrame(master=self, width=300, height=420)


    # ===========================    CONFIGURE KAFKA CLUSTER   ====================================
    def widgets_configure_server(self):
        self.fmr_w2_main = CTkFrame(master=self, width=400, height=420)
        self.fmr_w2_main.place(x=20, y=20)
        
        # Logo
        img_logo = CTkImage(Image.open(".\static\safire_white.png"), size=(360,98))
        lbl_logo = CTkLabel(master=self.fmr_w2_main, image=img_logo, text='')
        lbl_logo.place(relx=0.5, rely=0.22, anchor=CENTER)
        

        # ==============================   USE SAVED VALUES CheckBox   ==================================
        self.use_saved = BooleanVar(value=True)
        self.chk_use_saved = CTkCheckBox(
                master=self.fmr_w2_main, 
                text="Use saved values for zookeeper and brokers?",
                variable=self.use_saved,
                onvalue=True,
                offvalue=False,
                command=self.cmd_use_custom_ips
            )
        self.chk_use_saved.place(relx=0.5, rely=0.58, anchor=CENTER)
        


        self.btn_start_server = CTkButton(
            master=self.fmr_w2_main,
            text="Start Kafka Cluster",
            command=self.start_kafka_cluster
        )
        self.btn_start_server.place(relx=0.5, rely=0.8, anchor=CENTER)
        
        
        

    # ================================   DEFINE CUSTOM IPs WIDGETS   ==========================
    def widgets_custom_ips(self):
        # global display_ips
        if self.use_saved.get() == False:

            
            # Set Frame
            
            self.geometry("760x460")       
            self.frm_custom_ips.place(x=440, y=20)
            
            
            lbl_configure_kafka = CTkLabel(self.frm_custom_ips, text="Configure the local Kafka cluster")
            lbl_configure_kafka.place(relx=0.5, rely=0.05, anchor=CENTER)
            
            

            # ===================   Number of Brokers Spinbox  =====================
            self.spb_num_brokers_num_brokers = Spinbox(
                master=self.frm_custom_ips,
                width=100, 
                step_size=1, 
                min_value=1,
                max_value=5,
                command=self.check_number_brokers)
            self.spb_num_brokers_num_brokers.place(relx=0.5, y=60, anchor=CENTER)
            self.spb_num_brokers_num_brokers.set(1)
            

            
            # ===================   Is Zookeeper check box?   =====================            
            self.chk_is_zookeeper = CTkCheckBox(
                master=self.frm_custom_ips,
                text="Is this machine acting as the zookeeper?",
                variable=self.is_zookeeper,
                onvalue=True,
                offvalue=False
            )
            self.chk_is_zookeeper.place(relx=0.5, y=100, anchor=CENTER)
            self.chk_is_zookeeper.configure(state='disabled')
            
            # =======================   Is Broker 0 check box  =====================
            self.chk_is_broker_0 = CTkCheckBox(
                master=self.frm_custom_ips,
                text="Is this machine acting as broker ID 0?      ",
                variable=self.is_broker_0,
                onvalue=True,
                offvalue=False
            )
            self.chk_is_broker_0.place(relx=0.5, y=140, anchor=CENTER)
            self.chk_is_broker_0.configure(state='disabled')
            
            
            # ================================   ENTER IPs Button   ======================
            self.btn_enter_ips = CTkButton(
                master=self.frm_custom_ips, 
                text="Save current IPs", 
                command=lambda: [self.get_num_ips_required()])
            self.btn_enter_ips.place(relx=0.5, y=180, anchor=CENTER)
            
            
            # ================================   Custom IPs List label   ======================
            self.lbl_ips_display = CTkLabel(master=self.frm_custom_ips, text="Kakfa Cluster IPs:")
            self.lbl_ips_display.place(relx=0.5, y=210, anchor='n')
            
            
            # ================================  START KAFKA CLUSTER Button   ========================
            self.btn_create_cluster = CTkButton(
                master=self.frm_custom_ips, 
                text="Start Kafka Cluster",
                command=self.start_kafka_cluster
            )
            self.btn_create_cluster.place(relx=0.5, rely=0.9, anchor=CENTER)
            self.btn_create_cluster.configure(state='disabled')


        elif self.use_saved.get() == True:

            self.frm_custom_ips.place_forget()
            self.geometry(self.geom_size)
            
            
    def cmd_use_custom_ips(self):
        
        if self.use_saved.get() == False:
            self.btn_start_server.configure(state='disabled')
            try:
                
                self.lbl_ips_display.configure(text='Kakfa Cluster IPs:')
                pass
            except:
                pass
        else:
            self.btn_start_server.configure(state='normal')
            
    
        self.widgets_custom_ips()


    def check_number_brokers(self):
        self.btn_create_cluster.configure(state='disabled')
        self.lbl_ips_display.configure(text='Kakfa Cluster IPs:')
        num_brokers = self.spb_num_brokers_num_brokers.get() 
        if num_brokers == 1:
            self.chk_is_zookeeper.select()
            self.chk_is_zookeeper.configure(state='disabled')
            self.chk_is_broker_0.select()
            self.chk_is_broker_0.configure(state='disabled')
            self.btn_enter_ips.configure(text='Save current IPs')
            self.zookeeper_ip = self.current_ip
            self.broker_0_ip = self.current_ip
        else:
            self.chk_is_zookeeper.configure(state='normal')
            self.chk_is_broker_0.configure(state='normal')
            self.btn_enter_ips.configure(text='Enter IP values')


    def get_num_ips_required(self):
        # global display_ips
        correction_factor = 0
        
        enter_ips_params = {
            "type": [],
            "machine": [],
            "broker_id": []
        }

        self.ips = {
            "type": [],
            "broker_id": [],
            "broker_ip": []
        }
                
        if self.is_zookeeper.get() == True:
            self.zookeeper_ip = self.current_ip
            self.ips["type"].append("0")
            self.ips["broker_id"].append("0")
            self.ips["broker_ip"].append(self.current_ip)
        else:
            enter_ips_params["type"].append('0')
            enter_ips_params["machine"].append('zookeeper')
            enter_ips_params["broker_id"].append('0')
            
            
        if self.is_broker_0.get() == True:
            self.broker_0_ip = self.current_ip         
            self.ips["type"].append("1")
            self.ips["broker_id"].append("0")
            self.ips["broker_ip"].append(self.current_ip)
            correction_factor = 1
            
            
            
        for i in range(0, self.spb_num_brokers_num_brokers.get() - correction_factor):
            enter_ips_params["type"].append("1")
            enter_ips_params["machine"].append(f"broker_{i + correction_factor}")
            enter_ips_params["broker_id"].append(str(i + correction_factor))
            
        print(f"enter_ips_params: {enter_ips_params}")
        df_ips = pd.DataFrame(self.ips)
        # print(df_ips)
        df_ips.to_sql('ips', con=connection, if_exists='replace')
        
        if self.spb_num_brokers_num_brokers.get() > 1:
            self.enter_ips(params=enter_ips_params)
        else:
            
            display_ips = self.get_and_display_ips()[0]
            self.lbl_ips_display.configure(text=display_ips)            
            self.btn_create_cluster.configure(state='normal')


    def get_and_display_ips(self):
        ips = create_df_ips(connection)
        display_ips = ips[0]
        df_ips = ips[1]
        df_ips['type']      = df_ips['type'].astype(int).astype(str)
        df_ips['broker_id'] = df_ips['broker_id'].astype(int).astype(str)
        
        return display_ips, df_ips

    def enter_ips(self, params):
        
        ips_window = IPsWindow(
            params=params, 
            lbl_ips_display=self.lbl_ips_display, 
            btn_create_cluster=self.btn_create_cluster,
            connection=connection
        )
        
        ips_window.grab_set()
        
    def start_kafka_cluster(self):
        skc = StartKafkaCluster(
            connection=connection,
            topic_name=self.topic_name,
            current_ip=self.current_ip,
            is_zookeeper=self.is_zookeeper
        )
        df_ips = self.get_and_display_ips()[1]
        skc.start_kafka_cluster(df_ips=df_ips)
        
        
        
        brokers = list(df_ips[df_ips['type']=='1']['broker_ip'])
        print(brokers)
        rcp_kp = RcpKafkaProducer(brokers=brokers, topic_name=self.topic_name)

        t0 = Thread(target=rcp_kp.run_producer, daemon=True)
        t0.start()
                