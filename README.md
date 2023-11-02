# AQ_SpikeAlerts
 This is code to make a simple Text Alert System for Air Quality Spikes in Minneapolis.

## Background 

When Canadian wildfires blanketed US cities this summer, air quality rose to the forefront of public concern across the country. In Minneapolis, the fight for the East Phillips Urban Farm also raised air quality as an environmental justice issue in the public consciousness. Asthma and other health issues are clearly higher in Minneapolis neighborhoods which have more polluting facilities, particularly in North Minneapolis, a heavily Black neighborhood, and the East Phillips neighborhood in south Minneapolis, which has a high Indigenous and immigrant population.   

Federal regulations that monitor air quality at a regional level leave large gaps in data in terms of knowing what people in a particular block or neighborhood are exposed to. [Community-Based Air Quality Monitoring](https://www.georgetownclimate.org/articles/community-based-air-quality-monitoring-equitable-climate-policy.htm) (CBAQM) projects address those gap by monitoring air quality at a neighborhood level.

Community organizers concerned about air quality have also come up with a variety of ways of tracking data and using it to hold governments and industry accountable for the poison y put into our air.   In Pittsburgh, for example, [Smell PHG](https://smellpgh.org) crowdsources information about smells to track pollutants that pose health risks to residents. This is a crucial intervention because it treats people’s lived experiences as valid data. 

[The City of Minneapolis](https://www.minneapolismn.gov/government/programs-initiatives/environmental-programs/air-quality/) has engaged in CBAM by monitoring by putting up and maintaining [PurpleAir](https://map.purpleair.com/1/mAQI/a10/p604800/cC0#11/44.9368/-93.2834) monitors, a system which provides real-time readings of PM 2.5 readings. This is a very important investment. However, there is a gap between simply making data available to the public and making an active effort to deliver it to people who need it.  The Air Quality Alerts system sets out to set out to close the gap, by providing an easy way to get updates about bad air quality only when there is a significant spike. 

Air quality monitering initiatives usually emphasize long-term exposure. However, acute exposure at certain levels also presents significant health risks. Future iterations of this project could offer daily, weekly or monthly air quality reports, but this version chooses to focus on 'spikes', represent possible acute exposure events.  

We believe clean air is a human right. We believe communities deserve to know exactly what we are breathing in, when, and what effects it might have on our health. This alert system is intended to be a tool to facilitate awareness and capacity to fight against those that would treat marginalized  communities as sacrifice zones. 

## Functionality  

Users who want to receive spike alerts can fill out our [Survey](https://redcap.ahc.umn.edu/surveys/?s=YNHFFJRRADMT7HLD) and have their phone number and location of interest stored in a secure [REDCap](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5764586/) database hosted by the University of Minnesota. It’s likely most users will enter their home, although schools, work, a favorite park, or any other location would make sense. If anyone wants multiple locations, please fill out the survey twice.

The program queries the PurpleAir API and searches for spikes above a threshold ([35 micrograms/meter^3](https://www.epa.gov/pm-pollution/national-ambient-air-quality-standards-naaqs-pm) is the current EPA Standard). The value is a variable that can easily be changed/adjusted. When the system detects a spike, it sends a text to all subscribers within a certain distance of the monitor if they don't already have an active alert. The text links to the sensor on the PurpleAir Webmap.

When all alerts end for a user, an end of spike alert message is sent to the subscriber, detailing the length and severity of the event, and a unique reporting option through REDCap. This information is also archived for future reference.

## Installation Instructions

 For development or personal use

+ Clone this repository 
+ Install Pyton 3
+ Install Jupyter Lab/Notebook (optional for running notebooks)
+ Install  Python dependencies using your preferred environment manager ([Miniconda](https://docs.conda.io/projects/miniconda/en/latest/) works well!). View dependencies [here](https://github.com/RwHendrickson/AQ_SpikeAlerts/blob/main/Conda_Environment.yml)
+ Install prefered database manager (we use pgAdmin 4)
+ Configure database (If you are interested in working on our database, please reach out to us for credential information. If you'd like to create your own, create a PostgreSQL Database following instructions in [initializedb.sql](https://github.com/RwHendrickson/AQ_SpikeAlerts/blob/main/Scripts/database/initializedb.sql))
    + If not using our database, create and connect to your own database, add PostGIS extensions, then run intializedb.sql
    + Add your database credentials to the .env file
    + Run everything in the 1_QAQC  folder.  
+ Configure a [.env](https://github.com/RwHendrickson/AQ_SpikeAlerts/blob/main/.env.example) file
	+ Reach out to PurpleAir to get an API read key
 	+ REDCap tokens will need to be generated for the sign-up and report surveys - we're working on supplying templates for these
 	+ To have the texting option, you will need to set up a [Twilio account](https://www.twilio.com/en-us/sms/pricing/us) or modify code to work with another SMS service 


## Outstanding To-Dos: 
+ Clustering Spikes - When multiple monitors close together spike at once it should be treated as one spike
+ Health Thresholds (pm2.5) may be adjusted
+ Allow users to specify message frequency, radius of sensor interest, spike threshold?
+ Perhaps include information on nearby roadways/facilities
+ Create our own webmap for selecting sensors/location of interest?
+ Test and community feedback 

## Potential use and next steps:  
+ Funding and deployment: Long term use an deployment requires an organizational sponsor or community fundraising. 
	+ Estimated costs (calculated by google cloud estimater) 
        + Google Cloud storage: $51.01- $100/month
        + Compute engineer $12-35/ month
        + Texts via Twillio  $.0079/ text to use Twillio. At a list of 500 users, texted twice a day, upper limit of $240/month. 
            + Alternatively, alerts could also be sent out via email or social media to keep costs down. 
+ Building out of subscriber list
+ Health training /response. Subscribers should be educated on how to respond to alert messages in a. For instance, teachers and parents could sign up for alert so they are aware when it is not safe to have students/children outside. 
+ Organizing: while protecting ourselves from dangerous air quality in the moment is useful, our ultimate goal is to shut down the facilities and infrastructure that creates these problems. We can imagine this tool being integrated with organizing efforts in a variety of ways –  ultimately this information is only as powerful as the communities that use it. 

## Authors 

Priya Dalal-Whelan

Rob Hendrickson

Special Acknowledgements:

Mateo Frumholtz - thank you for your work on building and maintaining the REDCap Surveys

Jake Ford - thank you for brainstorming and helping out with project management

We also acknowledge the preliminary work of the [Quality Air, Quality Cities team](https://github.com/RTGS-Lab/QualityAirQualityCities).

