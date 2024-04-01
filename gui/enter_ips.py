
from typing import Tuple
import customtkinter as ctk
import tkinter as tk
from tkinter import ttk
from customtkinter import CTkButton, CTkFrame, \
    CTkEntry, CTkToplevel, CTkLabel, CENTER
    
from utils import create_df_ips

import ipaddress
import pandas as pd

# ===============================   ENTER IPs WINDOW   ===================================
class IPsWindow(CTkToplevel):
    def __init__(self, params, lbl_ips_display, btn_create_cluster, connection):
        super().__init__()
        
        self.title("Enter IP addresses")
        self.lbl_ips_display = lbl_ips_display
        self.btn_create_cluster = btn_create_cluster
        self.connection = connection
        self.params = params
        
        self.geom_size = "400x200"
        self.geometry(self.geom_size)

        self.count = 0       
        
        self.machines = list(self.params["machine"])
        
        self.lst_ips = []
        
        self.widgets_enter_ips(machine=self.machines[self.count])
        
        
        
    def widgets_enter_ips(self, machine, lbl_text="Enter IP address for {machine}:", text_color=None):
        
        frm_enter_ips = CTkFrame(master=self, width=360, height=160)
        frm_enter_ips.place(x=20, y=20)
        
        if text_color is None:
            text_color = "#FF851E"
        lbl_enter_ips = CTkLabel(frm_enter_ips, text=lbl_text.format(machine=machine), text_color=text_color)
        lbl_enter_ips.place(relx=0.5, rely=0.2, anchor=CENTER)

        self.ent_ip = CTkEntry(frm_enter_ips, width=200, placeholder_text=f"{machine} IP address")
        self.ent_ip.place(relx=0.5, rely=0.5, anchor=CENTER)
        
        btn_save_ip = CTkButton(frm_enter_ips, text=f"Save {machine} IP", command=lambda: [self.close_enter_ips(), None])
        btn_save_ip.place(relx=0.5, rely=0.8, anchor=CENTER)
        
    
    def close_enter_ips(self):
        # global display_ips

        try:
            br_ip = str(ipaddress.ip_address(self.ent_ip.get()))
            self.lst_ips.append(br_ip)
            # display_ips += self.machines[self.count] + ' IP: ' + br_ip + '\n'
            
            self.ips = {
                "type": [],
                "broker_id": [],
                "broker_ip": []
            }
            
            self.ips["type"].append(int(self.params["type"][self.count]))
            self.ips["broker_id"].append(int(self.params["broker_id"][self.count]))
            self.ips["broker_ip"].append(br_ip)

            df_ips = pd.DataFrame(self.ips)
            

            df_ips.to_sql('ips', con=self.connection, if_exists='append')

            self.count += 1
            if self.count < len(self.machines):
                print(self.machines, self.count)
                self.widgets_enter_ips(machine=self.machines[self.count])
                
            else:

                print(create_df_ips(self.connection)[0])
                self.lbl_ips_display.configure(text=create_df_ips(self.connection)[0])
                self.btn_create_cluster.configure(state='normal')
                self.destroy()
        
        except Exception as e:
            print(e)
            self.widgets_enter_ips(
                machine=self.machines[self.count], 
                lbl_text="Please enter a valid IP address for machine {machine}:", 
                text_color='red'
            )
        

