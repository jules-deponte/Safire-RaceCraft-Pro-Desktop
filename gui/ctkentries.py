
import customtkinter as ctk
from customtkinter import *


ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("safire")

class Entries(ctk.CTk):
    def __init__(
            self, *args,
            width: int = 150,
            height: int = 32,
            command: None,
            machine_type: None,
            **kwargs
        ):
        super().__init__(*args, **kwargs)

        self.machine_type = machine_type
        self.command = command

        # self.configure(fg_color=("gray78", "gray28"))  # set frame color

        self.grid_columnconfigure((0, 2), weight=0)  # buttons don't expand
        self.grid_columnconfigure(1, weight=1)  # entry expands

        self.lbl_ip = ctk.CTkLabel(self, text=f"IP address of the {self.machine_type}")
        self.lbl_ip.grid(row=0, column=0, padx=(20, 30), pady=3)

        self.ent_ip = ctk.CTkEntry(self, width=width-(2*height), height=height-6, border_width=0)
        self.ent_ip.grid(row=0, column=2, columnspan=1, padx=3, pady=3, sticky="ew")

        self.btn_confirm = ctk.CTkButton(self, text="✓", width=height-6, height=height-6,
                                                command=self.add_button_callback)
        self.btn_confirm.grid(row=0, column=3, padx=(0, 3), pady=3)

        # default value
        self.ent_ip.insert(0, "0.0.0.0")

    def add_button_callback(self):
        if self.command is not None:
            try:
                self.command()
            except ValueError:
                return
