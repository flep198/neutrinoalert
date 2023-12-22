from gpiozero import LEDBoard
from time import sleep
import time
import requests
import numpy as np
import pandas as pd
import csv, smtplib, ssl
from astropy.coordinates import SkyCoord
from tqdm import tqdm
import astropy.units as u
from datetime import datetime
from functools import partial
import subprocess
import os

#get neutrino alert table from AMON
def getNeutrinoAlert():
    url="https://gcn.gsfc.nasa.gov/amon_icecube_gold_bronze_events.html"
    html= requests.get(url).content
    df_list=pd.read_html(html,header=1)
    df=pd.DataFrame(df_list[-1])
    return df

def sendMail(sender_email,password,email,message,port=587,smtp_server="smtp.mail.de"):

    # Create a secure SSL context
    context = ssl.create_default_context()

    print("Starting server...")

    with smtplib.SMTP(smtp_server, port) as server:
        server.starttls(context=context)
        server.login(sender_email, password)

    server.sendmail(sender_email,email,message)
    print("Mail sent to "+ name + " ("+email+")")

#let the LEDs blink
def DoAlert(sleep_time=0.05,duration=60):
    #Initialize LED pins
    leds=LEDBoard(21,20,7,8,25,24,23,18,15)
    start_time=time.time()
    end_time=time.time()
    #play audio alert
    player=subprocess.Popen(["mplayer","alert.wav","-loop","10"],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    #do alert
    while (end_time-start_time)<duration:
        leds.value=(0,1,1,1,1,1,1,1,1)
        sleep(sleep_time)
        leds.value=(1,0,1,1,1,1,1,1,1)
        sleep(sleep_time)
        leds.value=(1,1,0,1,1,1,1,1,1)
        sleep(sleep_time)      
        leds.value=(1,1,1,0,1,1,1,1,1)
        sleep(sleep_time)      
        leds.value=(1,1,1,1,0,1,1,1,1)
        sleep(sleep_time)      
        leds.value=(1,1,1,1,1,0,1,1,1)
        sleep(sleep_time)
        leds.value=(1,1,1,1,1,1,0,1,1)
        sleep(sleep_time) 
        leds.value=(1,1,1,1,1,1,1,0,1)
        sleep(sleep_time)
        leds.value=(1,1,1,1,1,1,1,1,0)
        sleep(sleep_time)
        end_time=time.time()
    
#this sends an email with information about the latest neutrino event in AMON table
def sendInfoMail(password,update=False):
    print("Neutrino Alert!")
   
    #create information to send in email
    df_neutrinos=getNeutrinoAlert()
    new_neutrino=df_neutrinos.iloc[0]

    #find neutrino name
    neutrino_date=new_neutrino["Date"]
    for i in range(len(df_neutrinos)):
        df_neutrinos["Time UT"][i]=datetime.strptime(df_neutrinos["Time UT"][i],"%H:%M:%S.%f")
    neutrino_time=datetime.strptime(new_neutrino["Time UT"],"%H:%M:%S.%f")
    neutrino_name="IC"+neutrino_date.replace("/","")
    
    #add letter to the end of neutrino name
    neutrino_number=len(df_neutrinos[(df_neutrinos["Date"]==neutrino_date) & (df_neutrinos["Rev"]==0) & (df_neutrinos["Time UT"]<neutrino_time)])
    neutrino_letter=chr(neutrino_number+97).upper()
    neutrino_name=neutrino_name+neutrino_letter

    #read RFC catalog
    df_VLBI = pd.DataFrame(data=pd.read_table('VLBI_RFC_2022a.txt',delim_whitespace=True,dtype={'DecD':'str'}))
    df_VLBI["ra"]=df_VLBI["RAh"].astype(str)+":"+df_VLBI["RAm"].astype(str)+":"+df_VLBI["RAs"].astype(str)
    df_VLBI["decl"]=df_VLBI["DecD"].astype(str)+":"+df_VLBI["Decm"].astype(str)+":"+df_VLBI["Decs"].astype(str)

    #here, one could add a pre-selection of sources according to RA/DEC of the NeutrinoEvent

    #create SkyCoord objects
    obj_VLBI=SkyCoord(df_VLBI["ra"],df_VLBI["decl"],frame="icrs",unit=(u.hourangle,u.deg))
    obj_neutrino=SkyCoord(new_neutrino["RA [deg]"].astype(float),
            new_neutrino["Dec [deg]"].astype("float"),
            frame="icrs",unit=u.deg)

    sources=[]
    distance_to_neutrino=[]

    #search RFC catalog for sources within neutrino region (+30 arcmin systematic error)
    for i,rfc in tqdm(enumerate(obj_VLBI)):
        distance=obj_neutrino.separation(rfc).arcmin
        if distance<(new_neutrino["Error90 [arcmin]"]+30):
            sources=np.append(sources,df_VLBI["J2000name"][i])
            distance_to_neutrino=np.append(distance_to_neutrino,distance)
    
    inds=np.argsort(distance_to_neutrino)
    sources=sources[inds]
    distance_to_neutrino=distance_to_neutrino[inds]   

    message="""Subject: {subject}

    Hi {name}, 
    {statement}
    The currently available 90% confidence region contains the following {n_rfc} RFC sources:
    {source_list}

    You can also check the RFC VLBI calibrator database here: {rfc_url}
    And details about the alert reported via the GCN notice here: {gcn_url}

    Enjoy the rest of your day!
    Your TELAMON Neutrino Alert

    """

    if update:
        message=partial(message.format,subject="Update for Neutrino Event " + neutrino_name,
                statement="there has been an update to a previous neutrino event (" + neutrino_name + ").")
    else:
        message=partial(message.format,subject="New Neutrino Alert " + neutrino_name,
                statement="there has been a new IceCube-Neutrino alert reported (" + neutrino_name + ").")

    #send mail
    print("Opening contacts_file.csv")
    with open("contacts_file.csv") as file:
        reader = csv.reader(file)
        next(reader)
        for name, email in reader:
            source_list=""
            for j,source in enumerate(sources):
                source_list=source_list+"\n"+str(source)+" ("+"{:.2f}".format(distance_to_neutrino[j])+" arcmin from center)"
            rfc_url="http://astrogeo.org/cgi-bin/calib_search_form.csh?ra="+str(new_neutrino["RA [deg]"])+"d&dec="+str(new_neutrino["Dec [deg]"])+"d&num_sou=20&format=html"
            gcn_url="https://gcn.gsfc.nasa.gov/notices_amon_g_b/"+str(new_neutrino["RunNum_EventNum"])+".amon"
            message_to_send=message(name=name,n_rfc=len(sources),source_list=source_list,rfc_url=rfc_url,gcn_url=gcn_url)
            sendMail("neutrino.alert@mail.de",password,email,message_to_send)
        print("Mail sent to "+ name + " ("+email+")")

def getRFCsources_inCirc(df_VLBI,neutrino_ra,neutrino_dec,neutrino_ra_err,neutrino_dec_err):

    field_sources=df_VLBI[(df_VLBI["ra"]<(neutrino_ra+neutrino_ra_err[0])) 
                          & (df_VLBI["ra"]>(neutrino_ra+neutrino_ra_err[1])) 
                          & (df_VLBI["decl"]<(neutrino_dec+neutrino_dec_err[0])) 
                          & (df_VLBI["decl"]>(neutrino_dec+neutrino_dec_err[1]))][["J2000name","ra","decl"]]
    
    return [field_sources["J2000name"].values,field_sources["ra"],field_sources["decl"]]




def sendGCNMail(password,dataframe):
    print("Neutrino Alert!")
   
    #create information to send in email
    new_neutrino=dataframe.iloc[0]
    neutrino_name=new_neutrino["IC Name"]

    #read RFC catalog
    df_VLBI = pd.DataFrame(data=pd.read_table('VLBI_RFC_2022a.txt',delim_whitespace=True,dtype={'DecD':'str'}))
    df_VLBI["ra"]=df_VLBI["RAh"].astype(str)+":"+df_VLBI["RAm"].astype(str)+":"+df_VLBI["RAs"].astype(str)
    df_VLBI["decl"]=df_VLBI["DecD"].astype(str)+":"+df_VLBI["Decm"].astype(str)+":"+df_VLBI["Decs"].astype(str)

    #here, one could add a pre-selection of sources according to RA/DEC of the NeutrinoEvent

    #create SkyCoord objects    
    obj_VLBI = SkyCoord(df_VLBI["ra"], df_VLBI["decl"], frame="icrs",unit=(u.hourangle, u.deg))

    #convert RA/Dec columns to degrees
    df_VLBI["ra"]=obj_VLBI.ra.deg
    df_VLBI["decl"]=obj_VLBI.dec.deg

    
    ra=float(new_neutrino["RA"])
    ra_err=[float(new_neutrino["Ra_err_plus"]),float(new_neutrino["Ra_err_minus"])]
    dec=float(new_neutrino["Dec"])
    dec_err=[float(new_neutrino["Dec_err_plus"]),float(new_neutrino["Dec_err_minus"])]
    field_sources=getRFCsources_inCirc(df_VLBI,ra,dec,ra_err,dec_err)[0]
    field_sources_ra=getRFCsources_inCirc(df_VLBI,ra,dec,ra_err,dec_err)[1]
    field_sources_decl=getRFCsources_inCirc(df_VLBI,ra,dec,ra_err,dec_err)[2]

    gcn_url=new_neutrino["GCN_link"].split('"')[1]

    message="""Subject: {subject}

    Hi {name}, 
    there has been a new GCN Circular about the most recent neutrino event.
    The currently available 90% confidence region contains the following {n_rfc} RFC sources:
    {source_list}

    You can also check the RFC VLBI calibrator database here: {rfc_url}
    And details about the alert reported via the GCN Circular here: {gcn_url}

    Enjoy the rest of your day!
    Your TELAMON Neutrino Alert

    """

    message=partial(message.format,subject="Update for Neutrino Event " + neutrino_name)
 
    
    #send email
    port = 587 # For SSL
    smtp_server = "smtp.mail.de"
    sender_email = "neutrino.alert@mail.de"
        
    # Create a secure SSL context
    context = ssl.create_default_context()
    
    print("Starting server...")

    with smtplib.SMTP(smtp_server, port) as server:
        server.starttls(context=context)
        server.login("neutrino.alert@mail.de", password)
        print("Opening contacts_file.csv")
        with open("contacts_file.csv") as file:
            reader = csv.reader(file)
            next(reader)
            for name, email in reader:
                source_list=""
                for j,source in enumerate(field_sources):
                    source_list=source_list+"\n SNAM="+str(source)+" ; SLAM=" + field_sources_ra[j].split(":")[0] + " " + field_sources_ra[j].split(":")[1] + " " + field_sources_ra[j].split(":")[2] + "s ; SBET= " + field_sources_dec[j].split(":")[0] + " " + field_sources_dec[j].split(":")[1] + " " + field_sources_dec[j].split(":")[2] + '";'
                rfc_url="http://astrogeo.org/cgi-bin/calib_search_form.csh?ra="+str(new_neutrino["RA"])+"d&dec="+str(new_neutrino["Dec"])+"d&num_sou=20&format=html"
                message_to_send=message(name=name,n_rfc=len(field_sources),source_list=source_list,rfc_url=rfc_url,gcn_url=gcn_url)
                server.sendmail(sender_email,email,message_to_send)
                print("Mail sent to "+ name + " ("+email+")")



#start program and load initial neutrino alert frame
n_ini=len(getNeutrinoAlert())

#also load GCN circulars
df_circ=pd.DataFrame(data=pd.read_csv("GCN_circular_neutrinos.csv"))
n_circ_ini=len(df_circ)

#ask for email password:
password=input("Please enter your email password:")

#test alert
#sendInfoMail(password)
#DoAlert()

#let the program run
count=0
while True:
    try:    
        #check updated AMON alerts
        new_table=getNeutrinoAlert()
        n_new=len(new_table)
        
        #check if new AMON alert is available
        if n_new>n_ini:
            if new_table["Rev"][0]==0: #in this case, this is a completely new alert
                sendInfoMail(password)
                DoAlert()
            else: #in this case, this is only an update, to a previous alert
                sendInfoMail(password,update=True)
            n_ini=n_ini+1 

        #check updated GCN circular alerts
        os.system("python3 update_circular_alert.py")
        df_circ_new=pd.DataFrame(data=pd.read_csv("GCN_circular_neutrinos.csv"))
        n_circ_new=len(df_circ_new)
 
        #check if new GCN circular is available
        if n_circ_new>n_circ_ini:
            sendGCNMail(password,df_circ_new)
            n_circ_ini=n_circ_ini+1
    except:
        print("Connection error...")

    #send mail everyday to check if alert is running
    if count==0 or count>1439:
        count=0
        check_message="""Subject: Neutrino Check

        Hi Flo, 
    
        I am still running.

        Enjoy the rest of your day!
        Your TELAMON Neutrino Alert

        """

        sendMail("neutrino.alert@mail.de",password,"florian.eppel@gmx.de",check_message)

    #wait for one minute
    time.sleep(60)
    count+=1
