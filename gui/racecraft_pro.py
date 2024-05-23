from login_client import App
from racecraft_pro_client import RaceCraftProClient



if __name__ == "__main__":
    main = App(app_to_run=RaceCraftProClient)
    main.mainloop()