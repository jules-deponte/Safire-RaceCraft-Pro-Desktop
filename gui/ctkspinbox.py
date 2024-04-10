
import customtkinter as ctk
from customtkinter import *


ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("safire")

class Spinbox(ctk.CTkFrame):
    def __init__(
            self, *args,
            width: int = 100,
            height: int = 32,
            step_size: 1,
            command: None,
            min_value: None,
            max_value: None,
            **kwargs
        ):
        super().__init__(*args, width=width, height=height, **kwargs)

        self.step_size = step_size
        self.min_value = min_value
        self.max_value = max_value
        self.command = command

        # self.configure(fg_color=("gray78", "gray28"))  # set frame color

        self.grid_columnconfigure((0, 3), weight=0)  # buttons don't expand
        self.grid_columnconfigure(1, weight=1)  # entry expands

        self.lbl_num_brokers = ctk.CTkLabel(self, text="Number of brokers:")
        self.lbl_num_brokers.grid(row=0, column=0, padx=(20, 30), pady=3)

        self.subtract_button = ctk.CTkButton(self, text="-", width=height-6, height=height-6,
                                                    command=self.subtract_button_callback)
        self.subtract_button.grid(row=0, column=1, padx=(3, 0), pady=3)

        self.entry = ctk.CTkEntry(self, width=width-(2*height), height=height-6, border_width=0)
        self.entry.grid(row=0, column=2, columnspan=1, padx=3, pady=3, sticky="ew")

        self.add_button = ctk.CTkButton(self, text="+", width=height-6, height=height-6,
                                                command=self.add_button_callback)
        self.add_button.grid(row=0, column=3, padx=(0, 3), pady=3)

        # default value
        self.entry.insert(0, "0")

    def add_button_callback(self):
        if self.command is not None:
            try:
                value = int(self.entry.get()) + self.step_size
                if value >= self.max_value + self.step_size:
                    value -= self.step_size
                    return
                self.entry.delete(0, "end")
                self.entry.insert(0, value)
                self.command()
            except ValueError:
                return

    def subtract_button_callback(self):
        if self.command is not None:
            try:
                value = int(self.entry.get()) - self.step_size
                if value <= self.min_value - self.step_size:
                    value -= self.step_size
                    return
                self.entry.delete(0, "end")
                self.entry.insert(0, value)
                self.command()
            except ValueError:
                return

    def get(self):
        try:
            return int(self.entry.get())
        except ValueError:
            return None

    def set(self, value: int):
        self.entry.delete(0, "end")
        self.entry.insert(self.min_value, str(int(value)))
        
        
