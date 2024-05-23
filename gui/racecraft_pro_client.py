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
import requests
import webbrowser

from ctkspinbox import Spinbox
from enter_ips import IPsWindow

from rcp_kafka.kafka_producer import RcpKafkaProducer
from threading import Thread

from utils import create_df_ips




# This file creates a custom Tkinter window which allows the user to start connect
# to the F1 game and listen for the UDP packets it sends. It also creates a Kafka
# producer and sends the data to the Kafka cluster.


# The CTkTopLevel class is one which creates a new window in the same application.
class RaceCraftProClient(CTkToplevel):
    def __init__(self, master, topic_name, access_token, user_id):
        super().__init__(master)
        
        self.title("Start Session")
        
        self.topic_name = topic_name
        self.user_id = user_id
        self.access_token = access_token
        
        self.geom_size = "440x460"
        self.geometry(self.geom_size)
        self.resizable(0, 0)
        self.num_brokers = 1
        self.session = "start"
        
        
        self.widgets_configure_server()
        


    # ===========================    CONFIGURE KAFKA CLUSTER   ====================================
    def widgets_configure_server(self):
        self.fmr_w2_main = CTkFrame(master=self, width=400, height=420)
        self.fmr_w2_main.place(x=20, y=20)
        
        # Logo
        img_logo = CTkImage(Image.open(".\static\safire_white.png"), size=(360,98))
        lbl_logo = CTkLabel(master=self.fmr_w2_main, image=img_logo, text='')
        lbl_logo.place(relx=0.5, rely=0.22, anchor=CENTER)
        


        

        # Start listener to F1 Game, as well as the Kafka producer
        self.btn_start_server = CTkButton(
            master=self.fmr_w2_main,
            text="Start Session",
            command=self.start_kafka_producer
        )
        self.btn_start_server.place(relx=0.5, rely=0.8, anchor=CENTER)
        
        
        

        
    def start_kafka_producer(self):
        
        if self.session == "start":
            print(f"self.session: {self.session}")
            self.btn_start_server.configure(text="End Session")


            url = "http://127.0.0.1:5000/start_session"
            
            headers = {"Authorization": f"Bearer {self.access_token}"}


            # This GET request will send the JWT as a header, and the webapp will 
            # return the list of brokers which the producer needs.
            try:
                print(self.access_token)
                response = requests.get(
                    url=url,
                    headers=headers,
                    timeout=3
                )
                print(response.json())
                brokers = response.json()["brokers"]
            except Exception as e:
                print(e)
            
            
            rcp_kp = RcpKafkaProducer(brokers=brokers, topic_name=self.topic_name)

            t0 = Thread(target=rcp_kp.run_producer, daemon=True)
            t0.start()
            

            self.session = "end"
            
        else:
            print(f"self.session: {self.session}")
            try:
                self.btn_create_cluster.configure(text="Start Session")
            except AttributeError as e:
                pass
            self.btn_start_server.configure(text="Start Session")
            # Place Request to end session here.
            self.session = "start"
            
        