# On-Demand NeTEx to HRDF converter

This tool allows to convert the Swiss NeTEx exports containing the on-demand data into the proprietary HRDF format.
Containing only the On-Demand data.

To learn more about:

* Swiss NeTEX On-Demand export: https://opentransportdata.swiss/cookbook/netex-on-demand-verkehre/
* On-Demand in Switzerland: https://www.oev-info.ch/sites/default/files/2024-07/Fachkonzept%20On-Demand_v2.1_en.pdf
* HRDF: https://opentransportdata.swiss/cookbook/hafas-rohdaten-format-hrdf/
* HRDF On-Demand: https://opentransportdata.swiss/cookbook/timetable-cookbook/hrdf-on-demand-verkehre/

# Libraries

You need to install the following additional libraries:

* pandas: https://pandas.pydata.org/ (to read csvs and handle data)
* requests: https://pypi.org/project/requests/ (to retrieve data from url)
* paramiko: https://www.paramiko.org/ (for SFTP support)

# Creating an exe

Simply use pyinstaller: https://pyinstaller.org/en/stable/

```sh
pyinstaller -F main.py
```

To include the resources folder in the exe, and not require it to be in the same folder as the main.exe. 
This also implies not being able to change tha "ATTRIBUT" file.

```sh
pyinstaller -F --add-data="resources:resources" main.py
```

# Using the tool

To run the code you can pass 4 parameters:

* (--offers) the offers to limit the conversion to (if you know them)
    * Default: "" (all offers)
* (--from_url) the url to read the NeTEx file from
    * Default: https://data.opentransportdata.swiss/dataset/netex_tt_odv/permalink
* (--from_folder) if you have the input in a folder then you can provide the folder to read from - if given we ignore
  the url
    * Default: "" (no from_folder)
* (--to_folder) the folder to write the converted HRDF files to. We assume the folder already exists!
    * Default: "output" - will be created if it does not exist
* (--keep_output) whether or not to keep the tmp and output folders after code execution
    * Default: True
* (--ftp) the parameters of the ftp to upload the data to, a quadruple (URL, User, Password, Path)
    * Default: None
    * IMPORTANT: the URL should contain the protocol (sftp or ftps) and if not default port 21 also the port, e.g., ":
      33".
    * ALSO: The path is the relative directory path you want the zip to be uploaded to.
    * We do not support the insecure FTP protocol
    * If this parameter is not given the zip file will remain locally

Example if you want to use the defaults :

```sh
python main.py 
```

Example with parameters:

```sh
python main.py --offers "Publicar Appenzell" --from_folder "C:\\somewhere\\netex" --to_folder "C:\\somehwere\\hrdf" --ftp "sftp://abc.com:33,user,password,path"
```

# What the code does

1. Create all HRDF files required to model on-demand
2. Loads the NeTEx-On-Demand data from the following sources (if not stated
   otherwise): https://data.opentransportdata.swiss/dataset/netex_tt_odv/permalink
3. If a folder is given the NeTEx-On-Demand data is loaded from there
4. To store the downloaded file and for the purpose of unzipping we create a "tmp" folder where the code is run
5. Traverse the NeTEx file and fill in the HRDF-files accordingly
6. Zip the resulting folder (the file will be named <todays_date>_hrdf_odv)
7. (optionally) Upload the Zip file to the given FTP Server
8. If data was loaded from url, remove it, the temp folder and if output folder was created remove that as well.
9. If there was an FTP upload also remove the zip file

Caveats:

* The files attribut and zugart are hard-coded
* Certain information required for a proper HRDF are momentarily not part of the Swiss NeTEx data. We are working on
  changing that.
