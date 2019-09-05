# POS Report Nomenclature
When a report is downloaded from an online portal and dump in the `dags/pos/data/report_download` it needs to be rename based on a specific nomenclature. This will be mandatory for the program to work.  Indeed, the banner and the date will be based on the report title.
  
The file name structure look's like this `TYPE_NAME_DATE`.
  
Where,

 - `TYPE` represent the type of report, in this case it's always equal to `pos`.
 - `NAME` represent the name of the report, in this case it represent the banner acronym (3 characters).
	 -  Ex. walmart = wal  
 - `DATE` represent the report **starting date** of the report, NOT the date that the report was downloaded. The date follows this format: `YYYY-MM-DD`.  
	 - Ex. 2019-12-31  
  
Here's some exemples:
 - `pos_wal_2019-12-31`
 - `pos_cos_2018-01-05`
