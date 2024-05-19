
import customtkinter as ctk
from customtkinter import CTkButton, CTkFrame, \
    CTkEntry, StringVar, CTkLabel, CENTER, CTkImage
    
from PIL import ImageTk, Image

import requests

from define_kafka_cluster import DefineKafkaCluster

from utils import PasswordHash





# This file creates the login window for the Safire RaceCraft Pro desktop client.
# It creates the initial login screen, and runs the mainloop for the client.
# After the user has logged in, the next window will open, which will allow the 
# user to configure the Kafka cluster.

# Create the GUI with this module
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("safire")


# ========================   LOGIN   ===================================
class App(ctk.CTk):
    def __init__(self):
        # Initialize the app, and set the window geometry.
        super().__init__()
        self.geometry("440x320")
        self.title("Login")
        self.resizable(0, 0)

        # Call a separate function to create the widgets on the screen.
        self.widgets_login()

    def widgets_login(self):
        fmr_main = CTkFrame(master=self, width=400, height=280)
        fmr_main.place(x=20, y=20)
        
        # Logo
        img_logo = CTkImage(Image.open(".\static\safire_white.png"), size=(360,98))
        lbl_logo = CTkLabel(master=fmr_main, image=img_logo, text='')
        lbl_logo.place(relx=0.5, rely=0.22, anchor=CENTER)

        # Temp topic is used to pre-populate the login, for debugging. 
        temp_topic = StringVar(value="Jules")
        self.ent_name = CTkEntry(master=fmr_main, placeholder_text="Username", textvariable=temp_topic)
        self.ent_name.place(x=220, y=120, anchor=CENTER)

        temp_password = StringVar(value="Password1")
        self.ent_password = CTkEntry(master=fmr_main, show="•", placeholder_text="Password", textvariable=temp_password)
        self.ent_password.place(x=220, y=160, anchor=CENTER)
        
        self.lbl_credentials = CTkLabel(master=fmr_main, text="", anchor=CENTER)
        self.lbl_credentials.place(x=120, y=190)
        

        btn_login = CTkButton(master=fmr_main, text="Login", command=lambda: [self.login()])
        btn_login.place(x=220, y=250, anchor=CENTER)


    def open_window(self):
        # This button will "hide" the login window. The function calls the class DefineKafkaCluster
        # which is a new window which allows the user to configure the Kafka cluster.
        self.withdraw() 
        self.topic_name = self.ent_name.get()
        self.password = self.ent_password.get()
        
        top = DefineKafkaCluster(self, topic_name=self.topic_name, access_token=self.token, user_id=self.user_id)
        top.protocol("WM_DELETE_WINDOW", self.on_top_window_close)
        top.deiconify() 

    def on_top_window_close(self):
        # After the new window from the "open_window" function is open, the
        # login window is destroyed.
        self.deiconify() 
        self.destroy()
        



    def login(self):
        self.topic_name = self.ent_name.get()
        self.password = self.ent_password.get()
        
        
        pwh = PasswordHash()
        password = pwh.password_hash(self.password)
        
        self.token   = None
        self.user_id = None
        
        
        r = requests.post(
            url="http://127.0.0.1:5000/login_kafka_client", 
            json={
                "username": self.topic_name,
                "password": password
            },
            timeout=3
        )
        
            
        if r.json()['code'] == 200:
            self.open_window()
            
        else:
            self.lbl_credentials.configure(text="Incorrect credentials. Please try again.", text_color="red")
            


if __name__ == "__main__":
    main = App()
    main.mainloop()