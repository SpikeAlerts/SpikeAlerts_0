# AQ_SpikeAlerts
 This is code to make a simple Text Alert System for Air Quality Spikes in Minneapolis.

## Background 

When Canadian wildfires blanketed US cities this summer, air quality rose to the forefront of public concern across the country. In Minneapolis, the fight for the East Phillips Urban Farm also raised air quality as an environmental justice issue in the public consciousness. Asthma and other health issues are clearly higher in Minneapolis neighborhoods which have more polluting facilities, particularly in North Minneapolis, a heavily Black neighborhood, and the East Phillips neighborhood in south Minneapolis, which has a high Indigenous and immigrant population.   

Federal regulations that monitor air quality at a regional level leave large gaps in data in terms of knowing what people in a particular block or neighborhood are exposed to. [Community-Based Air Quality Monitoring](https://www.georgetownclimate.org/articles/community-based-air-quality-monitoring-equitable-climate-policy.htm) (CBAQM) projects address those gap by monitoring air quality at a neighborhood level.

Community organizers concerned about air quality have also come up with a variety of ways of tracking data and using it to hold governments and industry accountable for the poison y put into our air.   In Pittsburgh, for example, [Smell PHG](https://smellpgh.org) crowdsources information about smells to track pollutants that pose health risks to residents. This is a crucial intervention because it treats people’s lived experiences as valid data. 

[The City of Minneapolis] (https://www.minneapolismn.gov/government/programs-initiatives/environmental-programs/air-quality/) has engaged in CBAM by monitoring by putting up and maintaining [PurpleAir](https://map.purpleair.com/1/mAQI/a10/p604800/cC0#11/44.9368/-93.2834) monitors, a system which provides real-time readings of PM 2.5 readings. This is a very important investment. However, there is a gap between simply making data available to the public and making an active effort to deliver it to people who need it.  The Air Quality Alerts system sets out to set out to close the gap, by providing an easy way to get updates about bad air quality only when there is a significant spike. 

Air quality monitering initiatives usually emphasize long-term exposure. However, acute exposure at certain levels also presents significant health risks. Future iterations of this project could offer daily, weekly or monthly air quality reports, but this version chooses to focus on 'spikes', represent possible acute exposure events.  

We believe clean air is a human right. We believe communities deserve to know exactly what we are breathing in, when, and what effects it might have on our health. This alert system is intended to be a tool to facilitate awareness and capacity to fight against those that would treat marginalized  communities as sacrifice zones. 

## Functionality  

Users who want to receive spike alerts can be added the the database along with their cross streets of interest - it’s likely most users will enter their home, although schools, work, a favorite park, or any other location of interest would make sense. 

The program queries the PurpleAir API and searches for spikes above 28.8, which represents a normal or nonthreatening level of PM 2.5. (The value is a variable that can easily be changed/adjusted – a deployed version would likely only be triggered by a value that poses serious health risks). When the system detects a spike, it sends a  text to all subscribers whose cross streets are within a certain distance of the monitor. The text details the severity of the spike and the distance from the subsriber's cross-street of interest. 

When the spike ends, an end of spike alert message is sent to the subscribers who were originally alerted, detailing the length of the spike, the location of the spike, and the maximum reading of the spike. This information is also archived for future reference. Messages are filtered so that a user isn’t spammed by rapid sensor fluctuations. 



## Installation Instructions

 For development or personal use

+ Install Pyton 3 
+ Install Jupyter Notebook 
+ Install  Python dependencies using your preferred environment manager. View dependencies [here](https://github.com/RwHendrickson/AQ_SpikeAlerts/blob/main/Conda_Environment.yml)
+ Install prefered databse manager (we use pgAdmin 4)
+ Clone the repository 
+ Configure database (If you are interested in working on our database, please reach out to us for credential information. If you'd like to create your own, create a PostgreSQL Databse)
    + Open “db_credentials_template.txt”. Change the dummy variables in the file your own personal credentials. Rename the file to  “db_credentials.txt”
    + If using our database, do not rerun the 1_QAQC files. This will enter repeat data. 
    + If not using our database, create and connect to your own database, add PostGIS extensions, then run intialize_db.sql and everything in the 1_QAQC  folder.  
+ Configure Twilo ([reach out if you're interested in working with our Twilio account, or create your own at twilio.com) 
+ Create a twilio.env file on your local machine. Add the following, replacing with:    
        `export 'TWILIO_ACCOUNT_SID='replace with your account SID'
        export TWILIO_AUTH_TOKEN='replace with your authorization token’'
        Add *twilio.env  file to your .gitignore file `


## Outstanding To-Dos: 
+ Build and integrate a signup form - REDCap and Google Form 
+ When multiple monitors close together spike at once it should be treated as one spike
+ How to group sensors into an alert
+ Health Thresholds (pm2.5) should be adjusted 
+ Update Entity-Relationship Diagram
+ Allow users to specify message frequency, radius of sensor interest, spike threshold?
+ Refining the messaging
+ Perhaps include information on nearby roadways/facilities or hyperlink to the sensor on PurpleAir Website
+ Create a webmap for selecting sensors/intersection of interest?
+ Test and community feedback 

## Potential use and next steps:  
+ Funding and deployment: Long term use an deopllyment requires an organizational sponsor or community fundraisin. 
	+ Estimated costs (calculated by google cloud estimater) 
        + Google Cloud storage: $51.01- $100/month
        + Compute engineer $12-35/ month
        + Texts via Twillio  $.0079/ text to use Twillio. At a list of 500 users, texted twice a day, upper limit of $240/month. 
            + Alternatively, alerts could also be sent out via email or social media to keep costs down. 
+ Building out of subscriber list
+ Health training /response. Subscribers should be educated on how to respond to alert messages in a. For instance, teachers and parents could sign up for alert so they are aware when it is not safe to have students/children outside. 
+ Organizing: while protecting ourselves from dangerous air quality in the moment is useful, our ultimate goal is to shut down the facilities and infrastructure that creates these problems. We can imagine thi tool being integrated with organizing efforts in a variety of ways –  ultimately this ifnormation is  only as powerful as the communities that use it. 

## Authors 

Rob Hendrickson 
Priya Dalal-Whelan 

We also acknowledge the preliminary work of the [Quality Air, Quality Cities team.](https://github.com/RTGS-Lab/QualityAirQualityCities)

