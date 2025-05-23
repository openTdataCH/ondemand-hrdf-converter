import os  # Import the os module for interacting with the operating system
import shutil  # for moving files
import xml.etree.ElementTree as xml_etree  # Import xml.etree.ElementTree for XML parsing
from datetime import timedelta, date, datetime  # Import datetime and timedelta for handling date and time
from typing import List, Tuple, Union  # for functions' parameter typing
from xml.etree.ElementTree import ElementTree, Element

import pandas as pd  # Import pandas for data manipulation and analysis

# Declare an iterator to iterate through journeys/trips in fplan
fplan_trip_iterator = 0

# To avoid crossing the "normal" bitfield numbers, we start with the id 900000
bitfeld_starting_number = 900000

# Booking rule iterator
infotext_id = 900000000

# Region iterator
region_id = 1

# Pseudo stop ("virtuelle haltestelle") id iterator
pseudo_stop_id = 9500000

# Booking rule iterator storage
info_text_ids = [str]

# List of different stop types
hrdf_stop_types = ['SSI', 'SDI', 'SSS', 'SDS', 'SSD', 'SDD']

# List of HRDF file names
hrdf_files = ["fplan", "zugart", "attribut", "infotext", "region", "bahnhof", "bfkoord", "bhfart", "bitfeld",
              "eckdaten"]

# Days of the week
week_days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

# Keep the namespaces for the Netex file
namespace = {'siri': 'http://www.siri.org.uk/siri', 'gml': 'http://www.opengis.net/gml/3.2',
             '': 'http://www.netex.org.uk/netex'}

# Folder names
INPUT_FOLDER_NAME = "input"
PREVIOUS_FOLDER_NAME = "previous"


######### Auxiliary functions #############
# empty check
def is_nan_or_empty(value: str) -> bool:
    return value is None or pd.isna(value) or value == ""


# move the given file to the given destination folder
def move_file(file_path: str, destination_folder: str):
    # Check if the source file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"!ERROR! Source file does not exist: {file_path}")

    # Check if the destination folder exists and is a directory
    if not os.path.isdir(destination_folder):
        raise NotADirectoryError(f"!ERROR! Destination folder does not exist: {destination_folder}")

    # Extract the filename from the file path
    filename = os.path.basename(file_path)

    # Create the full path for the destination file
    destination_path = os.path.join(destination_folder, filename)

    # Move the file by renaming it
    shutil.move(file_path, destination_path)
    print(f"File moved successfully from {file_path} to {destination_path}")


# copy file from to path
def copy_file(source_path: str, destination_path: str):
    shutil.copy(source_path, destination_path)

    print(f"File copied from {source_path} to {destination_path}.")


# get the path to the "previous file" that was transformed. If the folder (and file) does not exist, create folder.
# return path or None.
def get_previous_file_name() -> str:
    previous_folder = os.path.join(os.getcwd(), PREVIOUS_FOLDER_NAME)

    # if previous folder exists and is folder
    previous_netex_file_name = None

    if os.path.exists(previous_folder) and os.path.isdir(previous_folder):
        # get the single file in it
        for filename in os.listdir(previous_folder):
            previous_netex_file_name = filename  # Get the full file path
    else:
        os.makedirs(previous_folder, exist_ok=False)

    return previous_netex_file_name


# load the data from the url and put into a tmp folder, if it's a zip, then unzip to tmp folder
# Return file path
def load_and_unzip_from_url(url: str) -> str:
    import requests
    import os

    print(f"[[[[[Loading and unzipping from url {url}")

    temp_folder = os.path.join(os.getcwd(), INPUT_FOLDER_NAME)

    # Create a tmp folder to store and unzip the downloaded data
    os.makedirs(temp_folder, exist_ok=True)
    print(f"Created {INPUT_FOLDER_NAME} folder (will be removed)")

    # Send a GET request to the URL
    response = requests.get(url, stream=True)

    # Check if the request was successful
    if response.status_code == 200:
        # Try to read the header for the "Location"
        if 'content-disposition' in response.headers:
            file_name = response.headers['content-disposition'].split("filename=")[1]
            file_extension = os.path.splitext(file_name)[1]
        else:
            raise ValueError("!ERROR! The response header did not contain the 'content-disposition'")

        # Write the file to the tmp folder
        file_path = os.path.join(temp_folder, file_name)
        with open(file_path, 'wb') as file:
            # Write the content to the file in chunks
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"File downloaded successfully: {file_name}")

        # if file extension is zip, unzip it
        if file_extension == '.zip':
            unzip_to_folder(file_path, temp_folder)
            os.remove(file_path)
            print(f"Successfully unzipped file to {temp_folder}")
    else:
        raise requests.exceptions.HTTPError(f"!ERROR! Failed to download file HTTP-Code: {response.status_code}")
    print("]]]]]")

    return temp_folder


# delete a directory and all of its contents
def remove_directory(directory_path: str):
    try:
        # Check if the directory exists
        if os.path.exists(directory_path):
            # Iterate over all items in the directory
            for root, dirs, files in os.walk(directory_path, topdown=False):
                # Remove all files
                for name in files:
                    file_path = os.path.join(root, name)
                    os.remove(file_path)
                # Remove all subdirectories
                for name in dirs:
                    dir_path = os.path.join(root, name)
                    os.rmdir(dir_path)

            # Finally, remove the main directory itself
            os.rmdir(directory_path)
        else:
            raise NotADirectoryError(f"!ERROR! Directory '{directory_path}' does not exist.")
    except Exception as e:
        raise NotADirectoryError(f"!ERROR! Error occurred while trying to delete directory: {e}")


# unzips the given zip-file to the given folder
def unzip_to_folder(file_path: str, output_folder: str):
    import zipfile

    # Create the output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Open the zip file
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        # Extract all the contents into the output folder
        zip_ref.extractall(output_folder)


# zip a given folder to the given path
def zip_folder(folder_path: str, output_zip_path: str):
    import zipfile

    # Create a zip file
    with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Walk through the folder and add files to the zip
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                # Create the complete file path
                file_path = os.path.join(root, file)
                # Add file to the zip file, using relative path from the folder_path
                zipf.write(file_path, os.path.relpath(file_path, folder_path))
    print(f"Folder '{folder_path}' has been zipped into '{output_zip_path}'.")


# upload a given file to the given ftp
def upload_to_ftp(file_path: str, ftp: dict[str, str]):
    from ftplib import FTP_TLS
    import paramiko

    remote_path = ftp['path'] + os.path.basename(file_path)

    if ftp['protocol'].lower() == 'ftps':
        # FTPS upload
        try:
            ftps = FTP_TLS()
            ftps.connect(ftp['url'], int(ftp['port']))
            ftps.login(user=ftp['user'], passwd=ftp['password'])
            ftps.prot_p()  # Set the data connection to be secure

            with open(file_path, 'rb') as file:
                ftps.storbinary(f'STOR {remote_path}', file)
                print(f"Uploaded '{file_path}' to '{remote_path}' via FTPS.")

            ftps.quit()
        except Exception as e:
            print(f"FTPS upload error: {e}")

    elif ftp['protocol'].lower() == 'sftp':
        # SFTP upload
        try:
            transport = paramiko.Transport((ftp['url'], int(ftp['port'])))
            transport.connect(username=ftp['user'], password=ftp['password'])

            sftp = paramiko.SFTPClient.from_transport(transport)

            if sftp is not None:
                sftp.put(file_path, remote_path)  # Upload the file
                print(f"Uploaded '{file_path}' to '{ftp['path']}' via SFTP.")
                sftp.close()  # Close the SFTP client
            else:
                raise ConnectionError("!ERROR! Failed to create SFTP client.")

            transport.close()
        except Exception as e:
            print(f"SFTP upload error: {e}")

    else:
        print("Unsupported protocol. Please use 'ftps' or 'sftp'.")


# returns difference in minutes between timestamps formatted as hh:mm:ss
def time_difference_in_minutes(from_time: str, to_time: str) -> int:
    # Function to normalize time strings
    def normalize_time(time_str: str):
        # Split the time string into hours, minutes, and seconds
        hours, minutes, seconds = map(int, time_str.split(':'))
        # Normalize the time to a timedelta object
        return timedelta(hours=hours, minutes=minutes, seconds=seconds)

    # Normalize the time strings into timedelta objects
    time1 = normalize_time(from_time)
    time2 = normalize_time(to_time)

    # Calculate the total minutes for each time
    total_minutes1 = time1.total_seconds() / 60
    total_minutes2 = time2.total_seconds() / 60

    # Calculate the difference in minutes
    difference = total_minutes2 - total_minutes1

    # If the difference is negative, it means crossing over to the next day
    if difference < 0:
        difference += 24 * 60  # Add 24 hours in minutes

    return int(difference)  # Return the difference as an integer


# changes timestamps from hh:mm:ss to 0hhmm
def time_to_compact_time(time: str) -> str:
    # Remove seconds and replace ':' with ''
    time = time[:-3].replace(':', '')

    # Ensure the string has the right number of digits (5 digits)
    time = prefix_with_zeros(time, 5)

    return time  # Return the compact time string


# transform the netex 2024-12-15T00:00:00 to hrdf 15.12.2024
def netex_date_to_hrdf_date(date_str):
    # Parse the date string into a datetime object
    date_object = datetime.fromisoformat(date_str)

    # Format the datetime object to the desired format
    formatted_date = date_object.strftime("%d.%m.%Y")

    return formatted_date


# takes an int or str and prefixes its absolute value with "0" until length is reached
def prefix_with_zeros(value_to_prefix: Union[int, str], length: int) -> str:
    # Ensure value_to_prefix is positive and convert to string
    if type(value_to_prefix) is int:
        value_to_prefix = abs(value_to_prefix)

    value_length = len(str(value_to_prefix))
    value_to_prefix_str = str(value_to_prefix)

    # Prefix with zeros until the desired length is reached
    while value_length < length:
        value_to_prefix_str = "0" + value_to_prefix_str
        value_length += 1

    return value_to_prefix_str  # Return the zero-padded string


# converts an integer string to its hex representation
def binary_to_hex(binary: str) -> str:
    # Convert binary to integer - add the prefixing 11 as per requirements for HRDF
    integer_value = int("11" + binary, 2)

    # Convert integer to hexadecimal string, removing the '0x' prefix
    hex_value = hex(integer_value)[2:]  # Get hex representation without '0x'

    # Convert to uppercase
    hex_value = hex_value.upper()

    # Convert integer to hex string and return it
    return hex_value


# writes the content to the given HRDF file in the given folder, if it's valid
def write_to_hrdf(to_folder: str, hrdf_file: str, content: str, append: bool):
    # Check if the hrdf_file is valid
    if hrdf_file in hrdf_files:
        write_to_file(to_folder, hrdf_file, content, append)  # Write content to the specified HRDF file
    else:
        raise ValueError(f"!ERROR! {hrdf_file} is not a known HRDF file.")


# writes the content to the given file in the given folder
def write_to_file(folder: str, file_name: str, content: str, append: bool):
    # Open the specified file in append mode with UTF-8 encoding
    file_path = folder + "/" + file_name

    if append:
        with open(file_path, 'a', encoding='utf-8') as file:
            file.write(content + '\r')  # Write the content followed by a carriage return
    else:
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(content + '\r')  # Write the content followed by a carriage return


# checks if point-tuple is in polygon (list of tuples)
def is_point_in_polygon(point: Tuple[float, float], polygon: List[Tuple[float, float]]) -> bool:
    x, y = point
    n = len(polygon)
    inside = False

    p1x, p1y = polygon[0]
    for i in range(n + 1):
        p2x, p2y = polygon[i % n]
        xinters = 0.0
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside  # Toggle the inside status
        p1x, p1y = p2x, p2y

    return inside  # Return whether the point is inside the polygon


######### HRDF-handling functions #############
# initialize all HRDF files to the given folder
def init_hrdf(to_folder: str):
    # Create a mapping dictionary for HRDF file headers
    hrdf_files_headers = {
        "fplan": "*F 03 1",
        "zugart": "*F 06 1",
        "attribut": "*F 09 1",
        "infotext": "*F 11 1",
        "region": "*F 45 1",
        "bahnhof": "*F 01 1",
        "bfkoord": "*F 02 1",
        "bhfart": "*F 30 1",
        "bitfeld": "*F 05 1",
        "eckdaten": "*F 04 1"
    }

    # Create HRDF files and write headers
    for hrdf_file in hrdf_files:
        write_to_hrdf(to_folder, hrdf_file, hrdf_files_headers[hrdf_file], False)

    print("zugart hardcoded")
    init_zugart(to_folder)  # Initialize zugart HRDF

    print("attribut recycled (originally from hrdf export 23.05.2025)")
    init_attribut(to_folder)  # Initialize attribut HRDF

    print("HRDF files initiated")  # Log completion message


# init the hard-coded zugart file
def init_zugart(to_folder: str):
    # FIXME: This is currently hard coded
    write_to_hrdf(to_folder, "zugart", "TEL 10   1  DRT      0 T     #104", True)
    write_to_hrdf(to_folder, "zugart", "<text>", True)
    write_to_hrdf(to_folder, "zugart", "<Deutsch>", True)
    write_to_hrdf(to_folder, "zugart", "class6 Bus", True)
    write_to_hrdf(to_folder, "zugart", "category104 DRT", True)
    write_to_hrdf(to_folder, "zugart", "<Englisch>", True)
    write_to_hrdf(to_folder, "zugart", "class6 Bus", True)
    write_to_hrdf(to_folder, "zugart", "category104 DRT", True)
    write_to_hrdf(to_folder, "zugart", "<Franzoesisch>", True)
    write_to_hrdf(to_folder, "zugart", "class6 Bus", True)
    write_to_hrdf(to_folder, "zugart", "category104 DRT", True)
    write_to_hrdf(to_folder, "zugart", "<Italienisch>", True)
    write_to_hrdf(to_folder, "zugart", "class6 Bus", True)
    write_to_hrdf(to_folder, "zugart", "category104 DRT", True)


# init the attribut by copying it to the to_folder from the resources.
# we use the pre-loaded attribut file in the resources folder, which originates from the HRDF-export (23.05.2025)
def init_attribut(to_folder: str):
    copy_file("resources/attribut", to_folder + "/attribut")


######### NeTEx-handling functions #############
def convert_from_netex(offers: list[str], netex_file_path: str, to_folder: str):
    global fplan_trip_iterator  # Make the trip iterator accessible globally

    print("Loading from NeTEx")  # Log loading message

    tree = xml_etree.parse(netex_file_path)  # Parse the XML file

    root = tree.getroot()  # Get the root element of the parsed XML

    print("  # Creating ECKDATEN")  # Log creation message
    create_eckdaten(root, to_folder)

    print("  # Creating BITFELD")  # Log creation message
    bitfields = create_and_return_bitfields(root, to_folder)  # Create bitfields

    # Find all FlexibleLine elements, which contain the name and booking info
    flexible_lines = root.findall('.//FlexibleLine', namespaces=namespace)

    # Find all AvailabilityCondition elements, containing time and date validity of a line/service
    availability_conditions = root.findall('.//AvailabilityCondition', namespaces=namespace)

    # Find all ServiceJourney elements, which serve as join elements between different information
    service_journeys = root.findall('.//ServiceJourney', namespaces=namespace)

    # Find all StopPlaces
    stop_places = root.findall('.//StopPlace', namespaces=namespace)

    # This loop aims at finding unique triples of FlexibleLine + ServiceJourneyPattern + AvailabilityCondition
    for flexible_line in flexible_lines:
        flexible_line_id = flexible_line.attrib.get('id')  # Get the ID of the flexible line
        flexible_line_name = flexible_line.find('.//Name', namespaces=namespace).text  # Get the name

        if len(offers) == 0 or (flexible_line_name in offers):
            print(f"--- Loading flexible line: {flexible_line_name}")  # Log loading message

            # the booking arrangements (CURRENTLY!) contain the attribut values
            # -> FIXME: after changes to netex-odv. Then BookingArr. are "real" infotext and attributs are in "Notes"!
            # ATTRIBUT - attributes for the given flexible line, these are aligned with the official "hints"
            booking_arrangements = flexible_line.findall('.//BookingArrangement', namespaces=namespace)

            attribute_codes = return_attribute_codes(booking_arrangements)

            # INFOTEXT - infotexts for the given flexible line
            print("  # Creating INFOTEXT")  # Log creation message
            infotext_ids = create_and_return_infotexts(booking_arrangements, flexible_line_name, to_folder)

            # To store the triples and tuples
            fplan_triples = []
            fplan_tuples = []

            # to store the pseudo stops
            pseudo_stops = pd.DataFrame()

            # FPLAN processing
            for service_journey in service_journeys:
                service_flexible_line_ref = (service_journey.find('.//FlexibleLineRef', namespaces=namespace)
                                             .attrib.get('ref'))
                service_availability_condition_ref = service_journey.find('.//AvailabilityConditionRef',
                                                                          namespaces=namespace).attrib.get('ref')
                service_journey_pattern_ref = (
                    service_journey.find('.//ServiceJourneyPatternRef', namespaces=namespace)
                    .attrib.get('ref'))

                if service_flexible_line_ref == flexible_line_id:
                    # Check if the fplan triple is new
                    is_new_triple = True

                    for fplan_triple in fplan_triples:
                        if (fplan_triple[0] == service_flexible_line_ref and fplan_triple[1] ==
                                service_availability_condition_ref and fplan_triple[2] ==
                                service_journey_pattern_ref):
                            is_new_triple = False  # Mark as not new if already exists
                            break

                    if not is_new_triple:
                        continue  # Skip to the next iteration if it's not new
                    else:
                        print(
                            f"  # Creating FPLAN for {service_flexible_line_ref + ' ' + service_availability_condition_ref + ' ' + service_journey_pattern_ref}")
                        fplan_triples.append([service_flexible_line_ref, service_availability_condition_ref,
                                              service_journey_pattern_ref])  # Append the new triple

                    # Check if the fplan tuple is new
                    is_new_tuple = True
                    if len(fplan_tuples) == 0:
                        fplan_tuples.append([service_flexible_line_ref, service_journey_pattern_ref])
                    else:
                        for fplan_tuple in fplan_tuples:
                            if (fplan_tuple[0] == service_flexible_line_ref and fplan_tuple[1] ==
                                    service_journey_pattern_ref):
                                is_new_tuple = False  # Mark as not new if already exists
                                break

                    if is_new_tuple:
                        fplan_tuples.append(
                            [service_flexible_line_ref, service_journey_pattern_ref])  # Append new tuple

                        print(f"    ## Creating BAHNHOF for {service_journey_pattern_ref}")  # Log creation message
                        pseudo_stops = create_and_return_bahnhof(flexible_line_name +
                                                                 " " + service_journey_pattern_ref.rsplit(':', 1)[
                                                                     -1],
                                                                 to_folder)
                        print("    ## Creating REGION")  # Log creation message
                        create_region_and_bfkoord(service_journey_pattern_ref, pseudo_stops, stop_places, root,
                                                  to_folder)

                    for availability_condition in availability_conditions:
                        availability_condition_id = availability_condition.attrib.get('id')
                        availability_condition_from = availability_condition.find('.//StartTime',
                                                                                  namespaces=namespace).text
                        availability_condition_to = availability_condition.find('.//EndTime',
                                                                                namespaces=namespace).text
                        availability_condition_bits = availability_condition.find('.//ValidDayBits',
                                                                                  namespaces=namespace).text

                        if service_availability_condition_ref == availability_condition_id:
                            for i in [0, 2, 4]:
                                fplan_trip_iterator = (fplan_trip_iterator + 1)

                                ## FPLAN - comment
                                write_to_hrdf(to_folder, "fplan", "% " + flexible_line_name + " " +
                                              service_journey_pattern_ref.rsplit(':', 1)[-1] + " " +
                                              hrdf_stop_types[i], True)
                                write_to_hrdf(to_folder, "fplan", "% " + availability_condition_from[:-3] + "-"
                                              + availability_condition_to[:-3] + " Uhr", True)

                                ## FPLAN - journey
                                prefixed_iterator = prefix_with_zeros(fplan_trip_iterator, 6)
                                time_difference = prefix_with_zeros(
                                    time_difference_in_minutes(availability_condition_from,
                                                               availability_condition_to),
                                    4)
                                write_to_hrdf(to_folder, "fplan", "*T " + prefixed_iterator + " " + "AST___"
                                              + " " + str(time_difference) + " " + "0060", True)

                                ## FPLAN - bitfield/cal
                                bitfeld_reference = bitfields["bitfield_id"][
                                    bitfields["bitfield_bit"] == availability_condition_bits].iloc[0]

                                write_to_hrdf(to_folder, "fplan", "*A VE                 " + str(bitfeld_reference),
                                              True)
                                write_to_hrdf(to_folder, "fplan", "*G TEL", True)

                                # FPLAN/ATTRIBUT - attributes
                                for attribute_code in attribute_codes:
                                    write_to_hrdf(to_folder, "fplan", "*A " + attribute_code, True)

                                # FPLAN/INFOTEXT - infotexts
                                for info_text_id in infotext_ids:
                                    write_to_hrdf(to_folder, "fplan", "*I ZZ                        " +
                                                  str(info_text_id), True)

                                # FPLAN - start/stop pseudo stop: only react to the starts and add also ends
                                write_to_hrdf(to_folder, "fplan", pseudo_stops["pseudo_stop_id"][
                                    pseudo_stops["pseudo_stop_type"] == hrdf_stop_types[i]].iloc[0] + " " +
                                              hrdf_stop_types[i] + "                          " +
                                              time_to_compact_time(availability_condition_from), True)

                                write_to_hrdf(to_folder, "fplan", pseudo_stops["pseudo_stop_id"][
                                    pseudo_stops["pseudo_stop_type"] == hrdf_stop_types[i + 1]].iloc[0] + " " +
                                              hrdf_stop_types[i + 1] + "                   " +
                                              time_to_compact_time(availability_condition_from), True)

                                write_to_hrdf(to_folder, "fplan", "", True)  # Newline
        else:
            print(f"Not loading: {flexible_line_name}")


def create_eckdaten(root: ElementTree, to_folder: str):
    validity_period = root.find('.//CompositeFrame//ValidBetween', namespaces=namespace)

    from_date_netex = validity_period.find('.//FromDate', namespaces=namespace).text
    from_date = netex_date_to_hrdf_date(from_date_netex)

    to_date_netex = validity_period.find('.//ToDate', namespaces=namespace).text
    to_date = netex_date_to_hrdf_date(to_date_netex)

    year = to_date[to_date.rfind(".") + 1:]

    write_to_hrdf(to_folder, "eckdaten", from_date + " Fahrplanstart", True)
    write_to_hrdf(to_folder, "eckdaten", to_date + " Fahrplanende", True)
    write_to_hrdf(to_folder, "eckdaten", "\"Angebotsplan " + year + "\"", True)


def create_and_return_bitfields(root: ElementTree, to_folder: str):
    global bitfeld_starting_number  # Make the starting number accessible globally

    bitfields = pd.DataFrame(columns=["bitfield_id", "bitfield_hex", "bitfield_bit"])  # Initialize DataFrame

    valid_day_bits = root.findall('.//ValidDayBits', namespaces=namespace)  # Find valid day bits

    for validDayBit in valid_day_bits:
        hex_of_bitfield = binary_to_hex(validDayBit.text)  # Convert to hex

        if len(bitfields) == 0:
            # Write the new bitfield if none exist
            write_to_hrdf(to_folder, "bitfeld", str(bitfeld_starting_number) + " " + hex_of_bitfield, True)

            bitfields.loc[len(bitfields)] = [bitfeld_starting_number, hex_of_bitfield, validDayBit.text]
        else:
            # Check all bitfields' hex values
            existed = False

            for index, row in bitfields.iterrows():
                if row['bitfield_bit'] == validDayBit.text:
                    existed = True  # Mark as existing if found
                    break

            # If the bitfield did not exist, create a new entry
            if not existed:
                bitfeld_starting_number += 1  # Increment the starting number
                bitfields.loc[len(bitfields)] = [bitfeld_starting_number, hex_of_bitfield, validDayBit.text]
                write_to_hrdf(to_folder, "bitfeld", str(bitfeld_starting_number) + " " + hex_of_bitfield, True)

    return bitfields  # Return the DataFrame of bitfields


# We extract only the ATTRIBUT code from the id of the booking arrangements
# (see the FIXME mentioned above, this needs fixing using NeTEx "Notes")
def return_attribute_codes(booking_arrangements: list[Element]) -> list[str]:
    codes = []

    for booking_arrangement in booking_arrangements:
        # extract the attribut/hint code from the id
        booking_arrangement_id = booking_arrangement.attrib.get('id')

        code = extract_attribute_code_from_id(booking_arrangement_id)

        if not is_nan_or_empty(code):
            codes.append(code)

    return codes

# extract the attribute code from a given id string
def extract_attribute_code_from_id(id: str) -> str:
    if not is_nan_or_empty(id):
        last_part = id.split(":")[-1]

        if not is_nan_or_empty(last_part):
            code = last_part.split("_")[0]

            if not is_nan_or_empty(code):
                return code

    return ""


def create_and_return_infotexts(booking_arrangements: list[Element], flexible_line_name: str, to_folder: str) -> list[
    str]:
    global infotext_id  # Make the infotext ID accessible globally
    infotext_ids = []  # Initialize list to store infotext IDs

    write_to_hrdf(to_folder, "infotext", "% " + flexible_line_name, True)  # Write header for infotext

    for booking_arrangement in booking_arrangements:
        # fixme: after the attribut codes have been moved this check may no longer be required
        # extract the attribut/hint code from the id
        booking_arrangement_id = booking_arrangement.attrib.get('id')
        code = extract_attribute_code_from_id(booking_arrangement_id)
        if is_nan_or_empty(code):
            infotext_id += 1  # Increment infotext ID

            booking_note = booking_arrangement.find('.//BookingNote', namespaces=namespace)  # Get booking note

            write_to_hrdf(to_folder, "infotext", str(infotext_id) + " " + booking_note.text, True)  # Write infotext

            infotext_ids.append(infotext_id)  # Append ID to the list

    write_to_hrdf(to_folder, "infotext", "", True)  # Newline

    return infotext_ids  # Return the list of infotext IDs


def create_and_return_bahnhof(flexible_line_name, to_folder):
    global pseudo_stop_id  # Make the pseudo stop ID accessible globally

    pseudo_stops = pd.DataFrame(
        columns=["flexible_line_name", "pseudo_stop_id", "pseudo_stop_type"])  # Initialize DataFrame

    for hrdf_stop_type in hrdf_stop_types:
        # Write the pseudo stop information to the bahnhof file
        write_to_hrdf(to_folder, "bahnhof", str(pseudo_stop_id) + "     " + flexible_line_name + " " + hrdf_stop_type,
                      True)

        pseudo_stops.loc[len(pseudo_stops)] = [flexible_line_name, str(pseudo_stop_id), hrdf_stop_type]

        pseudo_stop_id += 1  # Increment the pseudo stop ID

    return pseudo_stops  # Return the DataFrame of pseudo stops


def create_region_and_bfkoord(service_journey_pattern_ref, pseudo_stops, stop_places, root, to_folder):
    global region_id  # Make the region ID accessible globally

    # Find the ServiceJourneyPattern for the given ref to use for joining with FlexibleStopAssignment
    service_journey_patterns = root.findall('.//ServiceJourneyPattern', namespaces=namespace)

    for service_journey_pattern in service_journey_patterns:
        if service_journey_pattern.attrib.get('id') == service_journey_pattern_ref:
            scheduled_stop_point_ref = service_journey_pattern.find('.//ScheduledStopPointRef',
                                                                    namespaces=namespace).attrib.get('ref')

            flexible_stop_assignments = root.findall('.//FlexibleStopAssignment', namespaces=namespace)

            for flexible_stop_assignment in flexible_stop_assignments:
                scheduled_stop_point_ref_assignment = (
                    flexible_stop_assignment.find('.//ScheduledStopPointRef', namespaces=namespace).attrib.get('ref'))
                flexible_area_ref = flexible_stop_assignment.find('.//FlexibleAreaRef',
                                                                  namespaces=namespace).attrib.get('ref')

                if scheduled_stop_point_ref == scheduled_stop_point_ref_assignment:
                    flexible_areas = root.findall('.//FlexibleArea', namespaces=namespace)

                    for flexible_area in flexible_areas:
                        if flexible_area.attrib.get('id') == flexible_area_ref:
                            name = flexible_area.find('.//Name', namespaces=namespace)
                            polygon_element = flexible_area.find('.//gml:Polygon', namespaces=namespace)
                            polygon = []  # To store coordinates as a list of coordinate tuples

                            if polygon_element is not None:
                                coordinates = polygon_element.findall('.//gml:pos', namespaces=namespace)

                                write_to_hrdf(to_folder, "region",
                                              "*R " + prefix_with_zeros(region_id, 8) + " " + name.text, True)
                                write_to_hrdf(to_folder, "region", "*C 0", True)
                                write_to_hrdf(to_folder, "region", "*P +", True)
                                region_id += 1  # Increment the region ID

                                first_coordinate = True

                                for coordinate in coordinates:
                                    coordinate_parts = coordinate.text.split(" ")
                                    polygon.append(
                                        (float(coordinate_parts[0]), float(coordinate_parts[1])))  # Add to polygon

                                    if first_coordinate:
                                        print("    ## Creating BFKOORD")  # Log creation message

                                        for index, row in pseudo_stops.iterrows():
                                            write_to_hrdf(to_folder, "bfkoord", row["pseudo_stop_id"] +
                                                          "  " + coordinate.text + "  " + "% " +
                                                          row["flexible_line_name"] + " " + row["pseudo_stop_type"],
                                                          True)

                                        write_to_hrdf(to_folder, "bfkoord", "", True)  # Newline
                                        write_to_hrdf(to_folder, "bfkoord", "", True)  # Newline

                                        print("    ## Creating BHFART")  # Log creation message
                                        for index, row in pseudo_stops.iterrows():
                                            write_to_hrdf(to_folder, "bhfart", row["pseudo_stop_id"] + " " +
                                                          "B" + "  " + "7" + "  " + "0" + " " +
                                                          row["flexible_line_name"] + " " + row["pseudo_stop_type"],
                                                          True)
                                            write_to_hrdf(to_folder, "bhfart", row["pseudo_stop_id"] + " " +
                                                          "P" + " " + "% " + row["flexible_line_name"] + " " +
                                                          row["pseudo_stop_type"], True)
                                            write_to_hrdf(to_folder, "bhfart", row["pseudo_stop_id"] + " " +
                                                          "E" + " " + "T" + " " + "% " + row["flexible_line_name"] + " "
                                                          + row["pseudo_stop_type"], True)
                                        write_to_hrdf(to_folder, "bhfart", "", True)  # Newline

                                        first_coordinate = False

                                    write_to_hrdf(to_folder, "region", coordinate.text,
                                                  True)  # Write coordinate to region file

                                write_to_hrdf(to_folder, "region", "", True)  # Newline

                                for index, row in pseudo_stops.iterrows():
                                    write_to_hrdf(to_folder, "region", "*" + row["pseudo_stop_type"], True)
                                    write_to_hrdf(to_folder, "region", "*IS", True)
                                    if row["pseudo_stop_type"] != "SDS" and row["pseudo_stop_type"] != "SSD":
                                        write_to_hrdf(to_folder, "region", "*BAS", True)
                                    write_to_hrdf(to_folder, "region",
                                                  row["pseudo_stop_id"] + " " + "% " + row["flexible_line_name"], True)

                                write_to_hrdf(to_folder, "region", "", True)  # Newline

                                # To decide what to write in *AS and *AC, iterate the stops and do a point-in-polygon test
                                # TODO: This is a workaround and needs to be fixed!
                                write_as_ac_stops(stop_places, polygon, "*AS", to_folder)
                                write_to_hrdf(to_folder, "region", "", True)  # Newline

                                write_as_ac_stops(stop_places, polygon, "*AC", to_folder)
                                write_to_hrdf(to_folder, "region", "", True)  # Newline

                            else:
                                print(f"## {name.text} had no polygons")  # Log missing polygons message


def write_as_ac_stops(stop_places, polygon, as_or_ac, to_folder):
    first_stop_place = True  # Flag to check if it's the first stop place
    for stop_place in stop_places:
        # Get the reference type of the stop place
        type_of_place_ref = stop_place.find('.//TypeOfPlaceRef', namespaces=namespace).attrib.get('ref')

        if "regularStop" in type_of_place_ref:  # Check if it's a regular stop
            longitude = stop_place.find('.//Longitude', namespaces=namespace).text  # Get longitude
            latitude = stop_place.find('.//Latitude', namespaces=namespace).text  # Get latitude
            name = stop_place.find('.//Name', namespaces=namespace).text  # Get stop name
            stop_id = stop_place.find('.//PublicCode', namespaces=namespace).text  # Get stop ID

            point = (float(longitude), float(latitude))  # Create a point from coordinates

            is_in_polygon = is_point_in_polygon(point, polygon)  # Check if the point is in the polygon

            if is_in_polygon:
                if first_stop_place:
                    write_to_hrdf(to_folder, "region", as_or_ac, True)  # Write the header for AS or AC
                    first_stop_place = False  # Mark that the first stop place has been processed

                # Write stop ID and name to the region file
                write_to_hrdf(to_folder, "region", stop_id + " " + "% " + name, True)
                write_to_hrdf(to_folder, "bhfart", stop_id + " " + "P" + " " + "% " + name, True)  # Write to bhfart


######### MAIN functions #############
def main(offers: list[str], from_folder: str, to_folder: str, ftp: dict[str, str], keep_output_folder: bool):
    # Initialize HRDF files
    init_hrdf(to_folder)

    # if existent get the previous netex file, otherwise only create the "previous folder"
    previous_netex_file_name = get_previous_file_name()

    # extract the netex file from the given folder
    netex_file_path = None
    netex_file_name = None

    # Ensure only one Netex file is present in the folder
    if len(os.listdir(from_folder)) > 1:
        raise ValueError("!ERROR! More than one NeTEx file delivered.")

    # Iterate through the files in the specified folder (to actually get the single netex file path
    for netex_file_name in os.listdir(from_folder):
        netex_file_path = os.path.join(from_folder, netex_file_name)  # Get the full file path

    if netex_file_path is not None and netex_file_name is not None:
        if previous_netex_file_name != netex_file_name:
            # Convert based on the specified format
            convert_from_netex(offers, netex_file_path, to_folder)

            # remove the netex file from the output/to_folder folder.
            if os.path.isfile(netex_file_path):
                # move the new file to the previous folder, and, if given, delete the old previous
                if previous_netex_file_name is not None:
                    os.remove(os.path.join(os.path.join(os.getcwd(), PREVIOUS_FOLDER_NAME), previous_netex_file_name))

                move_file(netex_file_path, os.path.join(os.getcwd(), PREVIOUS_FOLDER_NAME))
            else:
                raise FileNotFoundError(f"!ERROR! Was not a file path: {netex_file_path}")

            # zip the results to a file
            zip_file_name = str(date.today()) + "_hrdf_odv.zip"
            zip_file_path = os.path.join(os.getcwd(), zip_file_name)
            zip_folder(to_folder, zip_file_path)

            # upload to ftp
            if ftp:
                upload_to_ftp(zip_file_path, ftp)

            # Clean up
            if not keep_output_folder:
                remove_directory(to_folder)
                print("Removed the to_folder (and its files)")
            if input_folder is not None:
                remove_directory(input_folder)
                print("Removed the tmp folder (and its files)")
            if ftp is not None and os.path.isfile(zip_file_path):
                os.remove(zip_file_path)
                print("Removed zip file")
        else:
            print("WARNING: Already loaded the given NeTEx file")

            # Clean up
            if not keep_output_folder:
                remove_directory(to_folder)
                print("Removed the to_folder (and its files)")
            if input_folder is not None:
                remove_directory(input_folder)
                print("Removed the tmp folder (and its files)")


if __name__ == '__main__':
    import argparse  # Import argparse for command-line argument parsing

    # Set up the argument parser
    parser = argparse.ArgumentParser(
        description='Convert Swiss NeTEx file with On-Demand data to HRDF for On-Demand. If not given, creates an input, '
                    'output, and previous folder. The previous folder contains the previously transformed file.'
                    'Only exports if file has change. Input (if not given) folder is deleted in any case.')
    parser.add_argument('--offers', type=str,
                        help='List of offers to include separated with ",". Default: all offers.',
                        default="")
    parser.add_argument('--from_url', type=str,
                        help='The URL to load the NeTEx data from (can handle ZIPs). Will create folder input!',
                        default="https://data.opentransportdata.swiss/dataset/netex_tt_odv/permalink")
    parser.add_argument('--from_folder', type=str,
                        help='Folder containing the NeTEx data (unzipped!). If empty use URL (with input folder). '
                             'Delete folder if created automatically (file can be found in previous). Default: empty.',
                        default="")
    parser.add_argument('--to_folder', type=str,
                        help='Folder to output the HRDF files. We assume the folder already exists! Default is "/output"',
                        default="output")
    parser.add_argument('--keep_output', type=str,
                        help='Whether or not to keep the output folder and its files after the process. Previous is always kept',
                        default="True")
    parser.add_argument('--ftp', type=str,
                        help='The FTP to upload the zipped HRDF files to, a quadruple of url,user,password,path')

    print('Parsing arguments')
    args = parser.parse_args()

    # make sure the to_folder exists
    if not (os.path.exists(args.to_folder) and os.path.isdir(args.to_folder)):
        print(f"to_folder {args.to_folder} does not exist, will create it")
        temporary_to_folder = os.path.join(os.getcwd(), args.to_folder)
        os.makedirs(temporary_to_folder, exist_ok=True)
        args.to_folder = temporary_to_folder

    # handle from_folder vs from_url
    input_folder = None
    if args.from_folder == "":
        print(f'Downloading NeTEx file from URL: {args.from_url}')
        input_folder = args.from_folder = load_and_unzip_from_url(args.from_url)

    # parse the offers into a list of strings
    if args.offers == "":
        print('Converting all offers')
        args.offers = []
    else:
        if isinstance(args.offers, str):  # Ensure args.offers is a string
            flattened_offers = [x.strip() for x in args.offers.split(',')]
            args.offers = flattened_offers
            print(f'Converting only the following offers: {args.offers}')
        else:
            raise TypeError(f"Expected string for offers, got {type(args.offers).__name__}")

    # parse the ftp data
    if isinstance(args.ftp, str):
        ftp_params = args.ftp.split(",")
        ftp_url = ftp_params[0]
        ftp_user = ftp_params[1]
        ftp_password = ftp_params[2]
        ftp_path = ftp_params[3]

        # make sure the path ends with "/"
        if not ftp_path.endswith("/"):
            ftp_path = (ftp_path + "/")

        # parse protocol and port (if given)
        ftp_url_components = ftp_url.split(":")
        ftp_protocol = ftp_url_components[0]
        if ftp_protocol.lower() == "sftp":
            ftp_port = "22"
        elif ftp_protocol.lower() == "ftps":
            ftp_port = "21"
        else:
            raise TypeError(f"Unsupported protocol: {ftp_protocol}")

        if len(ftp_url_components) == 3:
            ftp_port = ftp_url_components[2]
            ftp_url = ftp_url_components[1].split("//")[1]
        else:
            ftp_url = ftp_url_components[1].split("//")[1]

        args.ftp = {
            'url': ftp_url.strip(),
            'user': ftp_user.strip(),
            'password': ftp_password.strip(),
            'path': ftp_path.strip(),
            'protocol': ftp_protocol.strip(),
            'port': ftp_port.strip()
        }
    else:
        print("No FTP was given, will leave the result-ZIP locally.")

    keep_output = True

    if args.keep_output == "":
        print('Not keeping output')
    else:
        keep_output = bool(args.keep_output)

    try:
        # Call main function with arguments
        main(args.offers, args.from_folder, args.to_folder, args.ftp, keep_output)
    except Exception as e:
        # Raise any exceptions encountered during execution
        raise e
