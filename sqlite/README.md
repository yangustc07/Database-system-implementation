Setting up TPCH on SQLite
=========================

Thank *Neeraj* for providing this nice guide!

For those who haven't already, basic guide to setting up TPCH on SQLite if you want to check the actual query results. Assuming you're on a linux box (if you use Windows for the projects, I admire your audacity :P) :

1. Install SQLite if you don't have it
```bash
sudo apt-get install sqlite3
```

2. Download SQL script `ddl-schema.sql` to create schema
(from [http://www.cs.utoronto.ca/~ryanjohn/teaching/cscc43-s11/ddl-schema.sql](http://www.cs.utoronto.ca/~ryanjohn/teaching/cscc43-s11/ddl-schema.sql), modified)

3. `cd` to directory you want to work in. I'll call it `workDir` henceforth.

4. Move `ddl-schema.sql` to `workDir` and create init an empty DB 
```bash
workDir$ sqlite3 -init ddl-schema.sql tpch.db
```
This will init the db, create your tables, set up constraints etc, and finally, open up the sqlite3 shell. You can exit it for now. We'll use a script to add data. To exit the shell, type

    sqlite> .quit

4. Download the script `create.sh` that will prepare and import the data into your DB for you.

Prepping consists of removing the trailing | on each line.

5. Move the script to `workDir`. Don't forget to give it `x` permissions.
```bash
workDir$> mv ~/your_download_dir/createTables.sh .
workDir$> chmod +x create.sh
```
Modify the data path `tblDir` in that script.

7. Run your import script
```bash
workDir$> ./create.sh
```
Wait... 1G files can take some time to import.

8. You're good to go. Run your SQLite commands by opening up the shell:
```bash
workDir$> sqlite3 tpch.db
```
