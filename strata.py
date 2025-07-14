#!/usr/bin/env python3.11
import sys
sys.path.append("/opt/.manus/.sandbox-runtime")
from flask import Blueprint, request, jsonify, Response
import requests
import json
import time
import logging
import csv
import io
import os # Needed for file path
import re # Needed for street name parsing
from collections import defaultdict # For easier aggregation
from rapidfuzz import fuzz, process, utils # Added for fuzzy matching

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

strata_bp = Blueprint("strata", __name__)

# --- Suburb Validation Setup ---
NSW_SUBURBS = set()
SUBURBS_FILE_PATH = os.path.join(os.path.dirname(__file__), "nsw_suburbs_opendatasoft.csv")
FUZZY_MATCH_THRESHOLD = 85 # Threshold for fuzzy matching

def load_nsw_suburbs():
    if NSW_SUBURBS:
        return
    try:
        with open(SUBURBS_FILE_PATH, mode="r", encoding="utf-8") as infile:
            reader = csv.DictReader(infile, delimiter=";")
            count = 0
            for row in reader:
                suburb_name = row.get("Official Name Suburb")
                if suburb_name:
                    NSW_SUBURBS.add(suburb_name.strip().upper())
                    count += 1
            logger.info(f"Successfully loaded {count} NSW suburbs.")
    except FileNotFoundError:
        logger.error(f"NSW Suburbs file not found at {SUBURBS_FILE_PATH}")
    except Exception as e:
        logger.error(f"Error loading NSW suburbs: {e}")

load_nsw_suburbs()
# --- End Suburb Validation Setup ---

# --- Street Name Parsing Logic (Redesigned for Performance) ---
_SUFFIX_LIST = [
    "ROAD", "STREET", "AVENUE", "PARADE", "PLACE", "CRESCENT", "CIRCUIT", "CLOSE", "COURT", "DRIVE", "ESPLANADE", 
    "GROVE", "LANE", "LOOP", "MEWS", "RISE", "ROW", "SQUARE", "TERRACE", "WALK", "WAY", "CIR", "CL", "CRT", 
    "CRES", "ESP", "GR", "PDE", "PL", "RD", "ST", "AVE", "DR", "LANE", "WYND", "ALY", "ARC", "BVD", "CH", 
    "CNR", "CSWY", "CUT", "GDNS", "HWY", "KY", "LINK", "MALL", "PROM", "RES", "RIDE", "SQ", "TCE", "TRL", 
    "VIEW", "WALK", "YARD", "RDGE", "PKWY", "PASS", "HTS", "GLEN", "DALE", "BRAE", "BANK", "BLVD", "BYPA", 
    "BYWY", "CCT", "CDS", "CTR", "CON", "COVE", "CRSE", "DRWY", "EST", "FWY", "GRA", "KEY", "LDGE", "LK", 
    "LKT", "LOOP", "PLZA", "PT", "QUAY", "RAMP", "RDG", "RDS", "RTE", "RUN", "SERV", "SPUR", "STRA", "TRFY", 
    "TRK", "TUNL", "TURN", "VI", "VLLS", "VSTA", "WTRS", "CIRCL", "CRT", "ESPL", "EXTN", "HIGHWY", "HILL", 
    "HOLW", "JCT", "LNDG", "MNR", "MT", "PASSG", "PATH", "PIKE", "PLNS", "RNCH", "SHRS", "SPGS", "SQRE", 
    "STA", "TER", "TPKE", "TRCE", "TRAK", "TRFY", "TRL", "TUNL", "VIS", "VLY", "VWS", "WKWY", "XING", 
    "ALLEE", "ANX", "ARCADE", "BAYOO", "BCH", "BEND", "BLF", "BLFS", "BLVD", "BTM", "BYP", "CANYN", "CAPE", 
    "CAUSWAY", "CEN", "CNTR", "CNYN", "COR", "CORS", "CRK", "CURV", "CYN", "DL", "DM", "DV", "DRS", "ESTS", 
    "EXP", "EXPY", "EXT", "EXTS", "FALL", "FLD", "FLDS", "FLT", "FLTS", "FRD", "FRDS", "FRK", "FRKS", 
    "FRST", "FRY", "FT", "GTWY", "GV", "HARB", "HAVN", "HBR", "HGTS", "HIWY", "HL", "HLS", "HOLW", "HT", 
    "HVN", "HYW", "INLT", "IS", "ISLE", "ISS", "JCTN", "KNL", "KNLS", "KY", "KYS", "LAND", "LCK", "LCKS", 
    "LDG", "LF", "LGT", "LGTS", "LK", "LKS", "LN", "LNDG", "MDW", "MDWS", "ML", "MLS", "MNR", "MNRS", 
    "MSN", "MSSN", "MT", "MTIN", "MTN", "MTNS", "MTWY", "NCK", "OPAS", "ORCH", "OVL", "PARK", "PK", "PKY", 
    "PKWYS", "PLN", "PLNS", "PLZ", "PNE", "PNES", "PORT", "PR", "PRT", "PRTS", "PSGE", "PT", "PTS", "RADL", 
    "RAMP", "RD", "RDG", "RDGS", "RDS", "RIV", "RIVR", "RNCH", "ROW", "RPD", "RPDS", "RST", "RT", "RUE", 
    "RUN", "SHL", "SHLS", "SHR", "SHRS", "SKWY", "SLP", "SMT", "SPG", "SPGS", "SQ", "SQR", "SQRS", "SQS", 
    "ST", "STA", "STAT", "STN", "STR", "STRA", "STRM", "STRT", "STS", "SUMT", "TER", "TERR", "TR", "TRAF", 
    "TRFY", "TRK", "TRKS", "TRL", "TRLS", "TRNPK", "TRWY", "TUNL", "TPKE", "UN", "UNS", "UPR", "VALY", 
    "VDCT", "VIA", "VW", "VWS", "VIL", "VILL", "VILLG", "VILLI", "VLG", "VLGS", "VLY", "VLYS", "VSTA", 
    "VSTS", "WY", "XING", "XRD", "XRDS"
]
STREET_SUFFIXES_SET = set(_SUFFIX_LIST)

PREFIX_CLEANUP_REGEX = re.compile(
    r"^\s*" 
    r"(?:(?:UNIT|U|APT|SHOP|SUITE|STE|LEVEL|L|OFFICE|KIOSK|ROOM|RM|FLAT|FL)\s*\S+\s*[/\\-]?\s*)?" 
    r"(?:\d+\S*\s*[/\\-]?\s*)?" 
    r"", re.IGNORECASE
)

def parse_street_name_from_address_for_aggregation(address_str):
    if not address_str or not isinstance(address_str, str):
        return "UNKNOWN ADDRESS (EMPTY INPUT)"
    address_upper = address_str.upper().strip()
    cleaned_address = PREFIX_CLEANUP_REGEX.sub("", address_upper, count=1).strip()
    search_base = cleaned_address if cleaned_address else address_upper
    parts = search_base.split()
    for i in range(len(parts) -1, -1, -1):
        if parts[i] in STREET_SUFFIXES_SET:
            street_name_parts = parts[:i+1]
            if len(street_name_parts) > 1 and re.fullmatch(r"[\d\-/]+", street_name_parts[-2]):
                pass 
            parsed_name = " ".join(street_name_parts).strip()
            return parsed_name
    final_fallback_name = cleaned_address if cleaned_address else address_upper
    if not final_fallback_name.strip():
        final_fallback_name = "UNKNOWN ADDRESS (NO SUFFIX AND EMPTY AFTER CLEAN)"
    logger.warning(f"Could not find standard suffix in \t'{address_str}\t' (cleaned to \t'{search_base}\t'). Using: \t'{final_fallback_name.strip()}\t'")
    return final_fallback_name.strip()

API_URL = "https://portal.spatial.nsw.gov.au/server/rest/services/StrataHub/FeatureServer/0/query"
FIELDS_TO_RETRIEVE = ["planlabel", "address", "suburb", "postcode", "lga", "lottotal"]
MAX_RECORDS_PER_REQUEST = 1000

def fetch_strata_data(where_clause):
    fetch_start_time = time.time()
    result_offset = 0
    all_features = []
    while True:
        query_params = {
            "where": where_clause,
            "outFields": ",".join(FIELDS_TO_RETRIEVE),
            "returnGeometry": "false",
            "resultOffset": result_offset,
            "resultRecordCount": MAX_RECORDS_PER_REQUEST,
            "orderByFields": "lottotal DESC",
            "f": "json"
        }
        try:
            response = requests.get(API_URL, params=query_params, timeout=60)
            response.raise_for_status()
            data = response.json()
            if "error" in data:
                logger.error(f"Error querying API: {data.get('error')}")
                return None, f"API Error: {data.get('error', {}).get('message', 'Unable to complete operation')}"
            features = data.get("features", [])
            if not features:
                break
            all_features.extend([f["attributes"] for f in features])
            if not data.get("exceededTransferLimit", False):
                break
            else:
                result_offset += len(features)
                time.sleep(0.1)
        except requests.exceptions.Timeout:
            logger.error("API request timed out.")
            return None, "API request timed out."
        except requests.exceptions.RequestException as e:
            logger.error(f"Error making API request: {e}")
            return None, f"Error making API request: {e}"
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON response. Text: {response.text}")
            return None, "Error decoding API response."
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            return None, f"An unexpected error occurred: {e}"
    logger.info(f"[PROFILE] fetch_strata_data completed in {time.time() - fetch_start_time:.4f}s. Total features: {len(all_features)}")
    return all_features, None

def build_suburb_where_clause(suburb):
    if not suburb:
        return None, "Please provide a suburb name."
    suburb_upper = suburb.strip().upper()
    suburb_to_query = None
    validation_error = None

    if not NSW_SUBURBS:
        logger.warning("NSW Suburbs list is empty, attempting to reload.")
        load_nsw_suburbs()
        if not NSW_SUBURBS:
             return None, "Suburb validation list could not be loaded. Cannot validate suburb."

    # 1. Exact match attempt
    if suburb_upper in NSW_SUBURBS:
        suburb_to_query = suburb_upper
    else:
        # 2. Try stripping "(NSW)" and exact match again
        plain_suburb_name = re.sub(r'\s*\(NSW\)\s*$', '', suburb_upper, flags=re.IGNORECASE)
        if plain_suburb_name in NSW_SUBURBS:
            suburb_to_query = plain_suburb_name
        else:
            # 3. Fuzzy match fallback (if not a special postcode case that should skip fuzzy)
            # Special postcode cases are handled in get_combined_data, so we attempt fuzzy match here for all non-exact matches.
            if NSW_SUBURBS: # Ensure list is available for matching
                query_suburb_processed = utils.default_process(suburb_upper)
                # Convert NSW_SUBURBS set to list for process.extractOne
                match = process.extractOne(query_suburb_processed, list(NSW_SUBURBS), scorer=fuzz.WRatio, score_cutoff=FUZZY_MATCH_THRESHOLD, processor=utils.default_process)
                
                if match:
                    suburb_to_query = match[0] # This is the original uppercase name from NSW_SUBURBS
                    logger.info(f"Fuzzy matched input '{suburb}' to '{suburb_to_query}' with score {match[1]}.")
                    validation_error = None # Clear previous error if fuzzy match succeeds
                else:
                    logger.info(f"Fuzzy matching failed for input '{suburb}'. No match above threshold {FUZZY_MATCH_THRESHOLD}.")
                    validation_error = f"Invalid NSW Suburb: \"{suburb}\". Please enter a valid NSW suburb name from the official list."
            else:
                 validation_error = f"Invalid NSW Suburb: \"{suburb}\". Suburb list unavailable for fuzzy matching."
    
    # This block for special suburbs like MANLY was part of the original error handling.
    # It's now largely superseded by fuzzy matching or handled in get_combined_data for postcode fallbacks.
    # If validation_error is set at this point, it means neither exact nor fuzzy match worked.
    if validation_error:
        # The special check for MANLY etc. was to allow them despite a small test list.
        # With a full list and fuzzy matching, this specific warning might be less relevant
        # but keeping it for now to see behavior with fuzzy logic.
        if suburb_upper in ["MANLY", "CREMORNE", "NEWINGTON", "NEUTRAL BAY"]:
             logger.warning(f"Proceeding with {suburb_upper} despite validation error '{validation_error}' (special case or postcode fallback expected).")
             # We allow it to proceed so get_combined_data can try postcode fallback for these specific suburbs.
             # For other suburbs, the error from fuzzy matching (or lack thereof) will be returned.
        else:
            return None, validation_error # Return the error if not a special postcode fallback case

    # If suburb_to_query is set (either by exact or fuzzy match), build the clause
    if suburb_to_query:
        sanitized_suburb_sql = suburb_to_query.replace("-", " ").replace("\"", "\"\"") # Basic SQL sanitization
        where_clause = f"UPPER(suburb) LIKE UPPER('%{sanitized_suburb_sql}%')"
        return where_clause, None
    
    # Fallback if suburb_to_query is somehow not set but no validation_error was returned (should ideally not happen with current logic)
    # Or if it's one of the special cases that had an error but we let it pass for postcode search.
    if suburb_upper in ["MANLY", "CREMORNE", "NEWINGTON", "NEUTRAL BAY"] and validation_error:
        logger.info(f"Allowing {suburb_upper} to proceed to get_combined_data for potential postcode search, despite validation error: {validation_error}")
        # Return None for where_clause, but also None for error, so get_combined_data tries postcode.
        return None, None 

    return None, validation_error if validation_error else f"Internal error during suburb validation for \"{suburb}\""

def get_combined_data(suburb):
    suburb_upper = suburb.strip().upper() if suburb else ""
    final_data = []
    errors = []
    data_suburb = None
    data_postcode = None

    where_suburb, error_suburb = build_suburb_where_clause(suburb)

    # If build_suburb_where_clause returns an error, and it's not a special postcode fallback case, return the error immediately.
    if error_suburb and suburb_upper not in ["MANLY", "CREMORNE", "NEWINGTON", "NEUTRAL BAY"]:
        return None, error_suburb

    if where_suburb: # If a valid where_clause for suburb was built (exact or fuzzy match)
        data_suburb, error_fetch_suburb = fetch_strata_data(where_suburb)
        if error_fetch_suburb:
            errors.append(f"Suburb search error: {error_fetch_suburb}")
    elif error_suburb: # This means validation failed, but it might be a special case we let through for postcode search
        logger.warning(f"Suburb validation failed for {suburb}: {error_suburb}. Proceeding to check for postcode fallback.")

    # Postcode fallback logic for specific suburbs
    postcode_to_search = None
    if suburb_upper == "MANLY": postcode_to_search = 2095
    elif suburb_upper == "CREMORNE": postcode_to_search = 2090
    elif suburb_upper == "NEWINGTON": postcode_to_search = 2127
    elif suburb_upper == "NEUTRAL BAY": postcode_to_search = 2089

    if postcode_to_search:
        logger.info(f"Attempting postcode fallback search for {suburb_upper} with postcode {postcode_to_search}")
        where_postcode = f"postcode = {postcode_to_search}"
        data_postcode, error_fetch_postcode = fetch_strata_data(where_postcode)
        if error_fetch_postcode:
            errors.append(f"Postcode search error: {error_fetch_postcode}")
        
        combined_dict = {}
        if data_postcode: # Prioritize postcode data if available for these specific suburbs
            for item in data_postcode:
                key = item.get("planlabel")
                if key: combined_dict[key] = item
        
        # Add suburb data only if not already present from postcode search (to avoid duplicates)
        if data_suburb:
            for item in data_suburb:
                key = item.get("planlabel")
                if key and key not in combined_dict: 
                    combined_dict[key] = item
        final_data = list(combined_dict.values())
    else:
        final_data = data_suburb if data_suburb else []

    final_error_msg = "; ".join(errors) if errors else None

    if final_data:
        if final_error_msg: 
            logger.warning(f"Returning partial data for {suburb} due to errors: {final_error_msg}")
        return final_data, None # If we have data, suppress minor fetch errors for now
    
    # If no data, and there was an initial suburb validation error (and not overridden by successful postcode search)
    if not final_data and error_suburb:
        return None, error_suburb
    
    # If no data and other fetch errors occurred
    if not final_data and final_error_msg:
        return None, final_error_msg

    return final_data, final_error_msg # Should be ([], None) if no data and no errors


def aggregate_data_by_street(building_data_list, original_suburb_query):
    agg_start_time = time.time()
    if not building_data_list:
        logger.info(f"[PROFILE] aggregate_data_by_street (empty list): {time.time() - agg_start_time:.4f}s")
        return []
    street_aggregation = defaultdict(lambda: {"total_lots_on_street": 0, "property_count": 0, "suburb": original_suburb_query.upper()})
    total_parsing_time = 0
    for i, building in enumerate(building_data_list):
        if i % 500 == 0 and i > 0:
            logger.info(f"[PROFILE] Aggregating building {i}/{len(building_data_list)}. Current total_parsing_time: {total_parsing_time:.4f}s. Elapsed: {time.time() - agg_start_time:.4f}s")
        address = building.get("address")
        lots_str = building.get("lottotal")
        if not address or lots_str is None:
            continue
        try:
            lots = int(lots_str)
        except (ValueError, TypeError):
            continue
        parsing_start_time_item = time.time()
        street_name = parse_street_name_from_address_for_aggregation(address)
        item_parsing_duration = time.time() - parsing_start_time_item
        total_parsing_time += item_parsing_duration
        if item_parsing_duration > 0.001:
            logger.info(f"[PROFILE] Slow parse for \t'{address}\t' -> \t'{street_name}\t': {item_parsing_duration:.6f}s")
        street_aggregation[street_name]["total_lots_on_street"] += lots
        street_aggregation[street_name]["property_count"] += 1
    
    aggregated_list = []
    for street, data in street_aggregation.items():
        aggregated_list.append({
            "street_name": street,
            "total_lots_on_street": data["total_lots_on_street"],
            "property_count": data["property_count"],
            "suburb": data["suburb"]
        })
    aggregated_list.sort(key=lambda x: x["total_lots_on_street"], reverse=True)

    cumulative_lots_running_total = 0
    for item in aggregated_list:
        cumulative_lots_running_total += item["total_lots_on_street"]
        item["cumulative_lots"] = cumulative_lots_running_total
        
    logger.info(f"[PROFILE] aggregate_data_by_street completed in {time.time() - agg_start_time:.4f}s. Total parsing time within loop: {total_parsing_time:.4f}s. Aggregated {len(building_data_list)} buildings into {len(aggregated_list)} streets for suburb {original_suburb_query}.")
    return aggregated_list

@strata_bp.route("/search", methods=["GET"])
def search_strata():
    suburb = request.args.get("suburb")
    data, error = get_combined_data(suburb)
    if error:
        return jsonify({"error": error}), 400
    if data is None or not data:
         return jsonify([])
    return jsonify(data)

@strata_bp.route("/search_street_level", methods=["GET"])
def search_strata_street_level():
    endpoint_start_time = time.time()
    suburb = request.args.get("suburb")
    if not suburb:
        return jsonify({"error": "Suburb parameter is required."}), 400
    logger.info(f"Street level search initiated for suburb: {suburb}")
    building_data, error = get_combined_data(suburb)
    if error:
        logger.error(f"Error in get_combined_data for {suburb}: {error}")
        return jsonify({"error": error}), 400 
    if building_data is None or not building_data:
        logger.info(f"No building data found for {suburb}")
        return jsonify([])
    street_level_data = aggregate_data_by_street(building_data, suburb)
    logger.info(f"[PROFILE] /search_street_level endpoint for {suburb} completed in {time.time() - endpoint_start_time:.4f}s")
    return jsonify(street_level_data)


@strata_bp.route("/search_street_level_ge20_lots", methods=["GET"])
def search_strata_street_level_ge20_lots():
    endpoint_start_time = time.time()
    suburb = request.args.get("suburb")
    if not suburb:
        return jsonify({"error": "Suburb parameter is required."}), 400
    
    logger.info(f"Street level search (>=20 lots) initiated for suburb: {suburb}")
    building_data, error = get_combined_data(suburb)
    
    if error:
        logger.error(f"Error in get_combined_data for {suburb} (>=20 lots view): {error}")
        return jsonify({"error": error}), 400 
    
    if building_data is None or not building_data:
        logger.info(f"No building data found for {suburb} (>=20 lots view)")
        return jsonify([])

    # Step 1: Filter buildings to include only those with lottotal >= 20
    buildings_ge20_lots = [
        b for b in building_data 
        if b.get("lottotal") is not None and isinstance(b.get("lottotal"), (int, str)) and int(b.get("lottotal")) >= 20
    ]

    if not buildings_ge20_lots:
        logger.info(f"No buildings with >= 20 lots found in {suburb} for >=20 lots view")
        return jsonify([])

    # Step 2: Aggregate these filtered buildings by street
    # The aggregate_data_by_street function will now sum lots and count properties based only on these pre-filtered buildings.
    all_street_level_data = aggregate_data_by_street(buildings_ge20_lots, suburb)

    if not all_street_level_data:
        logger.info(f"No street level data aggregated from buildings with >=20 lots for {suburb} (>=20 lots view)")
        return jsonify([])

    # Step 3: Filter streets where the sum of lots (from >=20-lot buildings) is itself >= 20
    filtered_street_data = [
        street_info for street_info in all_street_level_data 
        if street_info.get("total_lots_on_street", 0) >= 20
    ]

    if not filtered_street_data:
        logger.info(f"No streets with >= 20 lots found for {suburb}")
        return jsonify([])

    # Recalculate cumulative_lots for the filtered and sorted list
    # The list is already sorted by total_lots_on_street (desc) by aggregate_data_by_street
    cumulative_lots_running_total = 0
    for item in filtered_street_data:
        cumulative_lots_running_total += item["total_lots_on_street"]
        item["cumulative_lots"] = cumulative_lots_running_total

    logger.info(f"[PROFILE] /search_street_level_ge20_lots endpoint for {suburb} completed in {time.time() - endpoint_start_time:.4f}s. Found {len(filtered_street_data)} streets with >= 20 lots.")
    return jsonify(filtered_street_data)


# Helper functions for export functionality
def parse_street_address_for_building_view(raw_address, raw_suburb):
    """Parse street address for building view display"""
    address_for_parsing = raw_address or ""
    item_suburb_name = raw_suburb or ""
    if item_suburb_name:
        suffix_comma = f", {item_suburb_name}"
        if address_for_parsing.upper().endswith(suffix_comma.upper()):
            address_for_parsing = address_for_parsing[:-len(suffix_comma)].strip()
        else:
            suffix_space = f" {item_suburb_name}"
            if address_for_parsing.upper().endswith(suffix_space.upper()):
                address_for_parsing = address_for_parsing[:-len(suffix_space)].strip()
    
    match = re.match(r'^(\d+[A-Za-z]?(?:-\d+[A-Za-z]?)?)\s+(.*)', address_for_parsing)
    street_number = float('inf')
    street_name_part = address_for_parsing.upper().strip()
    if match:
        number_part = match.group(1)
        primary_number_match = re.match(r'^\d+', number_part)
        street_number = int(primary_number_match.group(0)) if primary_number_match else float('inf')
        street_name_part = match.group(2).upper().strip()
    
    return {'number': street_number, 'name': street_name_part, 'original': address_for_parsing}


def get_buildings_ge20_lots_data(suburb):
    """Get buildings with >= 20 lots data for export"""
    all_building_data, error = get_combined_data(suburb)
    if error:
        return None, error
    
    if not all_building_data:
        return [], None
    
    # Filter buildings with >= 20 lots
    buildings_ge20_lots_filtered = [
        b for b in all_building_data 
        if b.get("lottotal") is not None and isinstance(b.get("lottotal"), (int, str)) and int(b.get("lottotal")) >= 20
    ]
    
    if not buildings_ge20_lots_filtered:
        return [], None
    
    # Calculate street-level sums for filtered buildings
    street_lot_sums_for_filtered_buildings = defaultdict(int)
    for building in buildings_ge20_lots_filtered:
        address_str = building.get("address", "")
        street_name_for_sum = parse_street_name_from_address_for_aggregation(address_str)
        street_lot_sums_for_filtered_buildings[street_name_for_sum] += int(building.get("lottotal", 0))
    
    # Add calculated sums and cumulative lots
    processed_buildings = []
    cumulative_lots_running_total = 0
    for building in buildings_ge20_lots_filtered:
        processed_building = building.copy()
        address_str = processed_building.get("address", "")
        street_name_for_sum = parse_street_name_from_address_for_aggregation(address_str)
        
        processed_building["sum_of_lots_per_street"] = street_lot_sums_for_filtered_buildings.get(street_name_for_sum, 0)
        
        cumulative_lots_running_total += int(processed_building.get("lottotal", 0))
        processed_building["cumulative_lots"] = cumulative_lots_running_total
        
        processed_buildings.append(processed_building)
    
    return processed_buildings, None


def get_street_level_data(suburb):
    """Get street level data for export"""
    building_data, error = get_combined_data(suburb)
    if error:
        return None, error
    
    if not building_data:
        return [], None
    
    street_level_data = aggregate_data_by_street(building_data, suburb)
    return street_level_data, None


def get_street_level_ge20_lots_data(suburb):
    """Get street level data with >= 20 lots for export"""
    building_data, error = get_combined_data(suburb)
    if error:
        return None, error
    
    if not building_data:
        return [], None
    
    # Filter buildings with >= 20 lots
    buildings_ge20_lots = [
        b for b in building_data 
        if b.get("lottotal") is not None and isinstance(b.get("lottotal"), (int, str)) and int(b.get("lottotal")) >= 20
    ]
    
    if not buildings_ge20_lots:
        return [], None
    
    # Aggregate filtered buildings by street
    all_street_level_data = aggregate_data_by_street(buildings_ge20_lots, suburb)
    
    if not all_street_level_data:
        return [], None
    
    # Filter streets where sum of lots >= 20
    filtered_street_data = [
        street_info for street_info in all_street_level_data 
        if street_info.get("total_lots_on_street", 0) >= 20
    ]
    
    if not filtered_street_data:
        return [], None
    
    # Recalculate cumulative lots
    cumulative_lots_running_total = 0
    for item in filtered_street_data:
        cumulative_lots_running_total += item["total_lots_on_street"]
        item["cumulative_lots"] = cumulative_lots_running_total
    
    return filtered_street_data, None


@strata_bp.route("/export", methods=["GET"])
def export_strata_csv():
    suburb = request.args.get("suburb")
    view_type = request.args.get("view", "building")  # Default to building view
    
    if view_type == "building":
        data, error = get_combined_data(suburb)
        if error:
            return jsonify({"error": error}), 400
        
        # Process data for building view with calculated columns
        if data:
            # Calculate street-level sums for building view
            street_name_lots_sum = {}
            for item in data:
                parsed_address = parse_street_address_for_building_view(item.get('address', ''), item.get('suburb', ''))
                street_name = parsed_address['name']
                lots = int(item.get('lottotal', 0))
                street_name_lots_sum[street_name] = street_name_lots_sum.get(street_name, 0) + lots
            
            # Sort by lots descending and add calculated columns
            sorted_data = sorted(data, key=lambda x: int(x.get('lottotal', 0)), reverse=True)
            cumulative_lots = 0
            export_data = []
            for i, item in enumerate(sorted_data):
                cumulative_lots += int(item.get('lottotal', 0))
                parsed_address = parse_street_address_for_building_view(item.get('address', ''), item.get('suburb', ''))
                street_name = parsed_address['name']
                
                export_row = {
                    'record_number': i + 1,
                    'planlabel': item.get('planlabel', ''),
                    'street_address_display': parsed_address['original'],
                    'suburb': item.get('suburb', ''),
                    'postcode': item.get('postcode', ''),
                    'lga': item.get('lga', ''),
                    'lottotal': item.get('lottotal', 0),
                    'sum_of_lots_per_street': street_name_lots_sum.get(street_name, 0),
                    'cumulative_lots': cumulative_lots
                }
                export_data.append(export_row)
            
            fieldnames = ['record_number', 'planlabel', 'street_address_display', 'suburb', 'postcode', 'lga', 'lottotal', 'sum_of_lots_per_street', 'cumulative_lots']
            data = export_data
        else:
            fieldnames = ['record_number', 'planlabel', 'street_address_display', 'suburb', 'postcode', 'lga', 'lottotal', 'sum_of_lots_per_street', 'cumulative_lots']
            data = []
            
    elif view_type == "building_ge20_lots":
        data, error = get_buildings_ge20_lots_data(suburb)
        if error:
            return jsonify({"error": error}), 400
        
        if data:
            export_data = []
            for i, item in enumerate(data):
                parsed_address = parse_street_address_for_building_view(item.get('address', ''), item.get('suburb', ''))
                
                export_row = {
                    'record_number': i + 1,
                    'planlabel': item.get('planlabel', ''),
                    'street_address_display': parsed_address['original'],
                    'suburb': item.get('suburb', ''),
                    'postcode': item.get('postcode', ''),
                    'lga': item.get('lga', ''),
                    'lottotal': item.get('lottotal', 0),
                    'sum_of_lots_per_street': item.get('sum_of_lots_per_street', 0),
                    'cumulative_lots': item.get('cumulative_lots', 0)
                }
                export_data.append(export_row)
            
            fieldnames = ['record_number', 'planlabel', 'street_address_display', 'suburb', 'postcode', 'lga', 'lottotal', 'sum_of_lots_per_street', 'cumulative_lots']
            data = export_data
        else:
            fieldnames = ['record_number', 'planlabel', 'street_address_display', 'suburb', 'postcode', 'lga', 'lottotal', 'sum_of_lots_per_street', 'cumulative_lots']
            data = []
            
    elif view_type == "street":
        data, error = get_street_level_data(suburb)
        if error:
            return jsonify({"error": error}), 400
        
        if data:
            export_data = []
            for i, item in enumerate(data):
                export_row = {
                    'record_number': i + 1,
                    'street_name': item.get('street_name', ''),
                    'property_count': item.get('property_count', 0),
                    'total_lots_on_street': item.get('total_lots_on_street', 0),
                    'cumulative_lots': item.get('cumulative_lots', 0)
                }
                export_data.append(export_row)
            
            fieldnames = ['record_number', 'street_name', 'property_count', 'total_lots_on_street', 'cumulative_lots']
            data = export_data
        else:
            fieldnames = ['record_number', 'street_name', 'property_count', 'total_lots_on_street', 'cumulative_lots']
            data = []
            
    elif view_type == "street_ge20_lots":
        data, error = get_street_level_ge20_lots_data(suburb)
        if error:
            return jsonify({"error": error}), 400
        
        if data:
            export_data = []
            for i, item in enumerate(data):
                export_row = {
                    'record_number': i + 1,
                    'street_name': item.get('street_name', ''),
                    'property_count': item.get('property_count', 0),
                    'total_lots_on_street': item.get('total_lots_on_street', 0),
                    'cumulative_lots': item.get('cumulative_lots', 0)
                }
                export_data.append(export_row)
            
            fieldnames = ['record_number', 'street_name', 'property_count', 'total_lots_on_street', 'cumulative_lots']
            data = export_data
        else:
            fieldnames = ['record_number', 'street_name', 'property_count', 'total_lots_on_street', 'cumulative_lots']
            data = []
    else:
        return jsonify({"error": "Invalid view type"}), 400
    
    # Generate CSV
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    if data:
        writer.writerows(data)
    csv_data = output.getvalue()
    output.close()
    
    filename_suburb = suburb.replace(" ", "_").replace("/", "-") if suburb else "export"
    filename = f"strata_export_{view_type}_{filename_suburb}.csv"
    
    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment;filename={filename}"}
    )





@strata_bp.route("/search_buildings_ge20_lots", methods=["GET"])
def search_buildings_ge20_lots():
    endpoint_start_time = time.time()
    suburb = request.args.get("suburb")
    if not suburb:
        return jsonify({"error": "Suburb parameter is required."}), 400
    
    logger.info(f"Filtered building search (>=20 lots) initiated for suburb: {suburb}")
    all_building_data, error = get_combined_data(suburb)
    
    if error:
        logger.error(f"Error in get_combined_data for {suburb} (buildings >=20 lots view): {error}")
        return jsonify({"error": error}), 400
    
    if all_building_data is None or not all_building_data:
        logger.info(f"No building data found for {suburb} (buildings >=20 lots view)")
        return jsonify([])

    # Step 1: Filter buildings to include only those with lottotal >= 20
    # The fetch_strata_data already sorts by lottotal DESC, so this filtered list will maintain that order initially.
    buildings_ge20_lots_filtered = [
        b for b in all_building_data 
        if b.get("lottotal") is not None and isinstance(b.get("lottotal"), (int, str)) and int(b.get("lottotal")) >= 20
    ]

    if not buildings_ge20_lots_filtered:
        logger.info(f"No buildings with >= 20 lots found in {suburb} for filtered building view")
        return jsonify([])

    # Step 2: Calculate "Sum of Lots per Street" based *only* on these filtered buildings
    # Group filtered buildings by street to calculate this sum
    street_lot_sums_for_filtered_buildings = defaultdict(int)
    for building in buildings_ge20_lots_filtered:
        address_str = building.get("address", "")
        # Use the same street parsing logic as street view for consistency in grouping
        street_name_for_sum = parse_street_name_from_address_for_aggregation(address_str)
        street_lot_sums_for_filtered_buildings[street_name_for_sum] += int(building.get("lottotal", 0))

    # Step 3: Add the calculated sum to each building and prepare the final list
    # The list is already sorted by lottotal DESC from fetch_strata_data
    processed_buildings = []
    cumulative_lots_running_total = 0
    for building in buildings_ge20_lots_filtered:
        # Create a copy to avoid modifying the original list items if they are referenced elsewhere
        processed_building = building.copy()
        address_str = processed_building.get("address", "")
        street_name_for_sum = parse_street_name_from_address_for_aggregation(address_str)
        
        processed_building["sum_of_lots_per_street"] = street_lot_sums_for_filtered_buildings.get(street_name_for_sum, 0)
        
        # Calculate cumulative lots for this filtered view
        cumulative_lots_running_total += int(processed_building.get("lottotal", 0))
        processed_building["cumulative_lots"] = cumulative_lots_running_total
        
        processed_buildings.append(processed_building)

    logger.info(f"[PROFILE] /search_buildings_ge20_lots endpoint for {suburb} completed in {time.time() - endpoint_start_time:.4f}s. Found {len(processed_buildings)} buildings with >= 20 lots.")
    return jsonify(processed_buildings)


