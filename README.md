# LoL-DB
A pair of files for producing a SQLite database for working with Leauge of Legends game data.

For all files, you need to enter your personal Riot API key in the myKey variable.

Run 'Table Create.py' first; it generates the raw database schema and populates (mostly) static data. The code as written comments out the generation functions, as there's also a function for refreshing tables that normally contain static data. Due to game patches, the static data may come out of date, but you can re-apply those tables without affecting any game data. 

'Games Data.py' is the function to produce a list of Master and Challenger players and return their match history, populating tables. Due to API limits, the program can take over 24 hours to run fully, but produces a very robust database. 
