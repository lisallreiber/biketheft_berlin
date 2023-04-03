# Daily data on Berlin bike thefts

> "Your bicylce just needs one more lock than the one next to it


There used to be a time when the average lifetime of my bicycles in Berlin was about one year - until they were stolen. This changed when I got a second lock because my local bicycle dealer suggested to get a second lock from another brand. Coincidence?

That's where my project comes in - to learn more about the key numbers and trends of bike theft in Berlin, I am developing a fully automated batch process that ingests [daily berlin bicycle theft data](https://daten.berlin.de/datensaetze/fahrraddiebstahl-berlin) reported by the police in Berlin, transforms it with the power of python and dbt, and stores it in a data warehouse (BigQuery). But why stop there? Using the power of Google Data Looker, the data is visualized, providing insights into the patterns and trends of bicycle theft in the city. Ready to take a ride with me and see how this all comes together? Let's go!


## Insights

- **Time trends**: Analyzing the temporal trends in bicycle theft can reveal patterns in the types of bikes stolen, the times of day when thefts are most likely to occur, and the days of the week when thefts are most common. This can help the community to take preventive measures during high-risk periods.

- **Hotspots**: By analyzing the location data of reported bicycle thefts, we can identify the areas in the city where bike theft is most common. This can help the authorities to allocate resources more effectively to prevent theft and catch thieves.

- **Bike characteristics**: Examining the characteristics of the bikes that are stolen, such as their make, model, and price, can provide insights into the types of bikes that are most targeted by thieves. This information can be used to raise awareness among bike owners and to help bike shops take measures to protect their inventory.


## Architecture

- **Data Source**: Every day, the berlin police uploads a csv file containing the most recent data on berlin bike thefts on one of the [Berlin Open Data Portals](https://daten.berlin.de/datensaetze/fahrraddiebstahl-berlin). The dataset includes information about the value of the bikes reported as stoled, the suspected timeframe in which the bike got stolen and the approximate location of the incident. 

- **Data Storage**: The data is first stored in a data lake (Google Cloud Storage) and then procecced and loaded into a data warehouse (BigQuery). Since this is an example project I am optimizing the storage by partitioning the data, even though this is not necessary at the current size of the dataset. 

- **Transformations**: (TODO: describe what transformations I am performing on the data using Python and dbt. What kind of cleaning and pre-processing steps are required before loading the data into the data warehouse?)

- **Visualization**: (TODO: What kind of insights am I hoping to gain from visualizing the data in Google Data Looker? Providing some examples of the types of visualizations I am creating -> how they will help to inform decision-making?)

- **Reproducibility**: (TODO: Describe how the project is reproducible and easy to run. Write specific instructions or prerequisites that users will need to follow to get the project up and running on their own.)
