import pandas as pd


def create_df_ips(connection):
    # Form the string to display the IP addresses on the interface.
    df_ips = pd.read_sql(sql="SELECT * FROM ips", con=connection)
    df_ips['type'] = df_ips['type'].astype(int).astype(str)
    df_ips['broker_id'] = df_ips['broker_id'].astype(int).astype(str)
    
    print(f"df_ips:\n\n{df_ips}\n\n")
    
    df_ips[df_ips['type']=='0']['broker_ip'].iloc[0]

    display_ips = 'Kafka Cluster IPs:\n\n'
    display_ips += f"zookeeper ID: {df_ips[df_ips['type']=='0']['broker_ip'].iloc[0]}\n"

    df_ips_br = df_ips[df_ips['type']=='1']
    df_ips_br = df_ips_br.sort_values(by=['broker_id'], ascending=[True])

    for i in range(len(df_ips_br)):
        display_ips += f"broker_{df_ips_br['broker_id'].iloc[i]} ID: {df_ips_br['broker_ip'].iloc[i]}\n"
    
    return display_ips, df_ips
    
