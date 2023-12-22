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

#import mimetypes

from email.utils import formataddr
from email.utils import formatdate
from email.utils import COMMASPACE

from email.header import Header
from email import encoders

from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

#get neutrino alert table from AMON
def getNeutrinoAlert():
    url="https://gcn.gsfc.nasa.gov/amon_icecube_gold_bronze_events.html"
    html= requests.get(url).content
    df_list=pd.read_html(html,header=1)
    df=pd.DataFrame(df_list[-1])
    return df

def send_email(passwd: str, sender_name: str, sender_addr: str, smtp: str, port: str,
        recipient_addr: list, subject: str, html: str, text: str,
        img_list: list=[], attachments: list=[],fn: str='last.eml', save: bool=False):

    sender_name=Header(sender_name, 'utf-8').encode()

    msg_root = MIMEMultipart('mixed')
    msg_root['Date'] = formatdate(localtime=1)
    msg_root['From'] = formataddr((sender_name, sender_addr))
    msg_root['To'] = COMMASPACE.join(recipient_addr)
    msg_root['Subject']= Header(subject, 'utf-8')
    msg_root.preamble = 'This is a multi-part message in MIME format.'

    msg_related = MIMEMultipart('related')
    msg_root.attach(msg_related)

    msg_alternative = MIMEMultipart('alternative')
    msg_related.attach(msg_alternative)

    msg_text = MIMEText(text.encode('utf-8'),'plain','utf-8')
    msg_alternative.attach(msg_text)

    msg_html = MIMEText(html.encode('utf-8'),'html','utf-8')
    msg_alternative.attach(msg_html)

    for i, img in enumerate(img_list):
        with open(img, 'rb') as fp:
            msg_image = MIMEImage(fp.read())
            msg_image.add_header('Content-ID','<image{}>'.format(i))
            msg_related.attach(msg_image)

    for attachment in attachments:
        fname = os.path.basename(attachment)

        with open(attachment, 'rb') as f:
            msg_attach = MIMEBase('application', 'octet-stream')
            msg_attach.set_payload(f.read())
            encoders.encode_base64(msg_attach)
            msg_attach.add_header('Content-Disposition', 'attachment',
                    filename=(Header(fname, 'uft-8').encode()))
            msg_root.attach(msg_attach)

    mail_server = smtplib.SMTP(smtp, port)
    mail_server.ehlo()

    try:
        mail_server.starttls()
        mail_server.ehlo()
    except smtplib.SMTPException as e:
        print(e)

    mail_server.login(sender_addr, passwd)
    mail_server.send_message(msg_root)
    mail_server.quit()

    if save:
        with(open(fn,'w')) as f:
            f.write(msg_root.as_string())


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

    message="""
    Hi {name}, <br /> 
    {statement}<br /> 
    The currently available 90% confidence region contains the following {n_rfc} RFC sources:<br /> 
    {source_list}<br /> <br /> 

    You can also check the RFC VLBI calibrator database here: {rfc_url}<br /> 
    And details about the alert reported via the GCN notice here: {gcn_url}<br /> <br /> 

    Enjoy the rest of your day!<br /> 
    Your TELAMON Neutrino Alert<br /> 

    """

    if update:
        subject="Update for Neutrino Event " + neutrino_name
        message=partial(message.format,statement="there has been an update to a previous neutrino event (" + neutrino_name + ").")
    else:
        subject="New Neutrino Alert " + neutrino_name
        message=partial(message.format,statement="there has been a new IceCube-Neutrino alert reported (" + neutrino_name + ").")

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
            
            #html formatting
            rfc_url="<a href="+rfc_url+">Link</a>"
            gcn_url="<a href="+gcn_url+">Link</a>"
            message_to_send=message(name=name,n_rfc=len(sources),source_list=source_list,rfc_url=rfc_url,gcn_url=gcn_url)
            send_email(password,"Neutrino Alert","neutrino.alert@mail.de",
                    "smtp.mail.de",587,[email],subject,message_to_send,message_to_send)
            print("Mail sent to "+ name + " ("+email+")")

def getRFCsources_inCirc(df_VLBI,neutrino_ra,neutrino_dec,neutrino_ra_err,neutrino_dec_err):

    field_sources=df_VLBI[(df_VLBI["ra_deg"]<(neutrino_ra+neutrino_ra_err[0])) 
                          & (df_VLBI["ra_deg"]>(neutrino_ra+neutrino_ra_err[1])) 
                          & (df_VLBI["decl_deg"]<(neutrino_dec+neutrino_dec_err[0])) 
                          & (df_VLBI["decl_deg"]>(neutrino_dec+neutrino_dec_err[1]))][["J2000name","ra","decl"]]
   
    return [field_sources["J2000name"].values,field_sources["ra"].values,field_sources["decl"].values]




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
    df_VLBI["ra_deg"]=obj_VLBI.ra.deg
    df_VLBI["decl_deg"]=obj_VLBI.dec.deg

    
    ra=float(new_neutrino["RA"])
    ra_err=[float(new_neutrino["Ra_err_plus"]),float(new_neutrino["Ra_err_minus"])]
    dec=float(new_neutrino["Dec"])
    dec_err=[float(new_neutrino["Dec_err_plus"]),float(new_neutrino["Dec_err_minus"])]
    field_sources=getRFCsources_inCirc(df_VLBI,ra,dec,ra_err,dec_err)[0]
    field_sources_ra=getRFCsources_inCirc(df_VLBI,ra,dec,ra_err,dec_err)[1]
    field_sources_decl=getRFCsources_inCirc(df_VLBI,ra,dec,ra_err,dec_err)[2]

    gcn_url=new_neutrino["GCN_link"].split('"')[1]
    gcn_url="<a href="+gcn_url+">Link</a>" 


    message="""
    Hi {name},<br /> 
    there has been a new GCN Circular about the most recent neutrino event.<br /> 
    The currently available 90% confidence region contains the following {n_rfc} RFC sources:<br /> 
    {source_list}<br /> <br /> 

    You can also check the RFC VLBI calibrator database here: {rfc_url}<br /> 
    And details about the alert reported via the GCN Circular here: {gcn_url}<br /> <br /> 

    Enjoy the rest of your day!<br /> 
    Your TELAMON Neutrino Alert<br /> 

    """

    subject="Update for Neutrino Event " + neutrino_name

    print("Opening contacts_file.csv")
    with open("contacts_file.csv") as file:
        reader = csv.reader(file)
        next(reader)
        for name, email in reader:
            source_list=""
            for j,source in enumerate(field_sources):
                source_list=source_list+"<br /> SNAM="+str(source)+" ; SLAM=" + field_sources_ra[j].split(":")[0] + " " + field_sources_ra[j].split(":")[1] + " " + field_sources_ra[j].split(":")[2] + "s ; SBET= " + field_sources_decl[j].split(":")[0] + " " + field_sources_decl[j].split(":")[1] + " " + field_sources_decl[j].split(":")[2] + '";'
            rfc_url="http://astrogeo.org/cgi-bin/calib_search_form.csh?ra="+str(new_neutrino["RA"])+"d&dec="+str(new_neutrino["Dec"])+"d&num_sou=20&format=html"
            
            #html formatting
            rfc_url="<a href="+rfc_url+">Link</a>"
                        
            message_to_send=message.format(name=name,n_rfc=len(field_sources),source_list=source_list,rfc_url=rfc_url,gcn_url=gcn_url)
            send_email(password,"Neutrino Alert","neutrino.alert@mail.de",
                    "smtp.mail.de",587,[email],subject,message_to_send,message_to_send)
            
            print("Mail sent to "+ name + " ("+email+")")


#also load GCN circulars
df_circ=pd.DataFrame(data=pd.read_csv("GCN_circular_neutrinos.csv"))
n_circ_ini=len(df_circ)

#ask for email password:
password=input("Please enter your email password:")

#test alert
#sendInfoMail(password)
#sendGCNMail(password,df_circ)
#DoAlert()

#let the program run
count=0
error_count_gcn=0
error_count_amon=0
while True:

    try:    
        try:
            n_ini
        except: 
            n_ini=len(getNeutrinoAlert())
        
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
    except:
        print("Connection error to AMON database")
        error_count_amon+=1
    try:
        #check updated GCN circular alerts
        os.system("python3 update_circular_alert.py")
        df_circ_new=pd.DataFrame(data=pd.read_csv("GCN_circular_neutrinos.csv"))
        n_circ_new=len(df_circ_new)
 
        #check if new GCN circular is available
        if n_circ_new>n_circ_ini:
            sendGCNMail(password,df_circ_new)
            n_circ_ini=n_circ_ini+1
    except:
        print("Connection error to GCN Notice database")
        error_count_gcn+=1

    #send mail everyday to check if alert is running
    if count==0 or count>1439:

        check_message="""
        Hi Flo, <br /> <br /> 
    
        I am still running.<br /> <br /> 
        {amon_info} % of my requests to AMON failed.<br />
        {gcn_info} % of my request to GCN failed.<br /><br />

        Enjoy the rest of your day!<br /> 
        Your TELAMON Neutrino Alert<br /> 

        """
        if count>0:
            check_message=check_message.format(amon_info="{:.2f}".format(error_count_amon/count*100),
                    gcn_info="{:.2f}".format(error_count_gcn/count*100))
        else:
            check_message=check_message.format(amon_info="{:.2f}".format(0.0),gcn_info="{:.2f}".format(0.0))

        send_email(password,"Neutrino Alert","neutrino.alert@mail.de",
                    "smtp.mail.de",587,["florian.eppel@uni-wuerzburg.de"],"Daily Neutrino Alert Check",check_message,check_message)

        #reset counts
        count=0
        error_count_gcn=0
        error_count_amon=0

    #wait for one minute
    time.sleep(60)
    count+=1
