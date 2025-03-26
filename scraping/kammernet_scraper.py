import json
import pandas as pd
from bs4 import BeautifulSoup
from base_scraper import BaseScraper

class KammernetScraper(BaseScraper):
    def __init__(self, city="nederland", use_proxy=False):
        super().__init__(base_url=None, use_proxy=use_proxy)
        self.city = city
        self.url = "https://kamernet.nl/services/api/listing/findlistings"

        self.listing_type_mapping = {
            1: 'Room',
            2: 'Apartment',
            4: 'Studio',
            8: 'Anti-squat',
            16: 'Student Housing'
        }

        self.headers = {
            "cookie": "ha_anonymous_id=b7590ca6-bbf3-4657-b6e8-88c6bcbde5ed; logonUser=; ASP.NET_SessionId=zjeo5321sdwvhukrj2tp45yz",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.5",
            "baggage": "sentry-environment=production,sentry-release=1538eb230e,sentry-public_key=667b19b66a985640c5e49e90d96a7537,sentry-trace_id=b9473d479f124adaafae85ba1e9aff65",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "Cookie": 'ha_anonymous_id=b7590ca6-bbf3-4657-b6e8-88c6bcbde5ed; _hjSessionUser_1248238=eyJpZCI6ImFmZDQ0MWY0LWFiYzktNThhNy05N2VmLTIzNTMzODU2NmM0ZCIsImNyZWF0ZWQiOjE3MzUzMzE4OTg2MTgsImV4aXN0aW5nIjp0cnVlfQ==; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Mar+26+2025+05%3A19%3A13+GMT%2B0100+(Central+European+Standard+Time)&version=202402.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=365f9b09-65fd-4381-86c5-a90cc383646d&interactionCount=2&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0004%3A1&AwaitingReconsent=false&geolocation=%3B; rl_anonymous_id=%22c8a3bbb1-bc36-4d16-a26e-e69003b582eb%22; rl_page_init_referrer=%22%24direct%22; rl_session=%7B%22id%22%3A1742962363558%2C%22expiresAt%22%3A1742965011074%2C%22timeout%22%3A1800000%2C%22autoTrack%22%3Atrue%2C%22sessionStart%22%3Afalse%7D; g_state={"i_p":1742771403161,"i_l":3}; kn-mp-source=Direct; OptanonAlertBoxClosed=2025-03-15T10:08:47.661Z; _gcl_au=1.1.94069107.1742033328; _ga=GA1.1.1373393383.1742032161; _ga_12G9JSB0C6=GS1.1.1742051983.3.1.1742051985.58.0.0; mp_a35ca7b30180789f75ad8610d4a9c8a4_mixpanel=%7B%22distinct_id%22%3A%20%22%24device%3A19409d768d2c10-02f8c31a7832458-f565723-186a00-19409d768d2c10%22%2C%22%24device_id%22%3A%20%2219409d768d2c10-02f8c31a7832458-f565723-186a00-19409d768d2c10%22%2C%22mp_lib%22%3A%20%22Rudderstack%3A%20web%22%2C%22%24search_engine%22%3A%20%22google%22%2C%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%2C%22__mps%22%3A%20%7B%7D%2C%22__mpso%22%3A%20%7B%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%7D%2C%22__mpus%22%3A%20%7B%7D%2C%22__mpa%22%3A%20%7B%7D%2C%22__mpu%22%3A%20%7B%7D%2C%22__mpr%22%3A%20%5B%5D%2C%22__mpap%22%3A%20%5B%5D%7D; ASP.NET_SessionId=mt0qrynoj5w4asgotrwnzl0o; logonUser=',
            "Origin": "https://kamernet.nl",
            "Referer": "https://kamernet.nl/huren/huurwoningen-nederland?searchview=1&maxRent=0&minSize=2&radius=5&pageNo=2&sort=1",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "sentry-trace": "b9473d479f124adaafae85ba1e9aff65-99b3bd2a997ef0a8-0",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0"
        }

        self.payload_template = {
            "location": None,
            "radiusId": 5,
            "listingTypeIds": [],
            "maxRentalPriceId": 0,
            "surfaceMinimumId": 2,
            "listingSortOptionId": 1,
            "pageNo": 1,
            "suitableForGenderIds": [],
            "furnishings": [],
            "availabilityPeriods": [],
            "availableFromDate": None,
            "isBathroomPrivate": None,
            "isToiletPrivate": None,
            "isKitchenPrivate": None,
            "hasInternet": None,
            "suitableForNumberOfPersonsId": None,
            "candidateAge": None,
            "suitableForStatusIds": [],
            "isSmokingInsideAllowed": None,
            "isPetsInsideAllowed": None,
            "roommateMaxNumberId": None,
            "roommateGenderIds": [],
            "ownerTypeIds": [],
            "variant": None,
            "searchview": 1,
            "rowsPerPage": 17,
            "OpResponse": {
                "Code": 1000,
                "Message": "Operation successful.",
                "HttpStatusCode": 200
            },
            "LogEntryId": None
        }

    def fetch_listings_page(self, page: int) -> dict:
        """
        Fetches and parses a single page's JSON data from Kamernet.
        Returns a dict:
          {
            'total_listings': int,
            'listings': [list of dicts],
            'success': bool
          }
        If the request fails or data is missing, 'success' = False.
        """
        # Copy the template and override the pageNo
        payload = dict(self.payload_template)
        payload["pageNo"] = page

        # Make the POST request (via BaseScraper.make_request)
        resp = self.make_request(self.url, method="POST", json=payload, headers=self.headers)
        if not resp:
            self.logger.error(f"Failed to fetch page {page}.")
            return {"success": False, "listings": [], "total_listings": 0}

        data = resp.json()

        # Extract 'listings' and 'total'
        total = data.get("total", 0)
        listings_raw = data.get("listings", [])

        # Convert each raw listing into a uniform dict
        listings_converted = []
        for listing in listings_raw:
            listing_type_id = listing.get("listingType", 0)
            record = {
                "listing_id": listing["listingId"],
                "street": listing["street"],
                "city": listing["city"],
                "surface_area": listing["surfaceArea"],
                "total_rental_price": listing["totalRentalPrice"],
                "availability_start": listing["availabilityStartDate"],
                "availability_end": listing.get("availabilityEndDate", "N/A"),
                "furnishing_id": listing["furnishingId"],
                "utilities_included": listing["utilitiesIncluded"],
                "student_house": listing["isStudentHouseAdvert"],
                "listing_type_id": listing_type_id,
                "listing_type_name": self.listing_type_mapping.get(listing_type_id, "Unknown")
            }
            listings_converted.append(record)

        return {
            "success": True,
            "total_listings": total,
            "listings": listings_converted
        }

    def collect_all_pages(self, max_pages=None) -> list:
        # Collect all pages or until max_pages and return list of dict records for each listing

        all_extracted_data = []
        page = 1
        fetched_total = 0
        total_listings = None

        while True:
            page_data = self.fetch_listings_page(page)
            if not page_data["success"]:
                self.logger.error(f"Stopping due to fetch error on page {page}.")
                break

            if total_listings is None:
                total_listings = page_data["total_listings"]
                self.logger.info(f"Total listings available: {total_listings}")

            listings = page_data["listings"]
            if not listings:
                self.logger.info("No more listings found, stopping.")
                break

            all_extracted_data.extend(listings)
            fetched_total += len(listings)
            self.logger.info(f"Fetched {len(listings)} from page {page}; total so far: {fetched_total}")

            # Stop if we've fetched everything or reached max_pages
            if fetched_total >= total_listings:
                self.logger.info("Fetched all available listings.")
                break
            if max_pages and page >= max_pages:
                self.logger.info(f"Reached max_pages = {max_pages}. Stopping.")
                break

            page += 1

        return all_extracted_data

    @staticmethod
    def build_page_link(row):
        return (
            "https://kamernet.nl/en/for-rent/"
        f"{row['listing_type_name'].lower().replace(' ', '-')}-"
        f"{row['city'].lower().replace(' ', '-')}/"
        f"{row['street'].lower().replace(' ', '-')}/"
        f"{row['listing_type_name'].lower().replace(' ', '-')}-"
        f"{row['listing_id']}"
        )

    def parse_listing_detail(self, url: str) -> dict:
        # Use our make_request method from BaseScraper to get the page
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
        }
        response = self.make_request(url, method="GET", headers=headers)
        if not response:
            self.logger.error(f"Failed to fetch detail page: {url}")
            return {}

        # Parse the HTML using BeautifulSoup
        soup = BeautifulSoup(response.text, "html.parser")
        script_tag = soup.find("script", {"id": "__NEXT_DATA__", "type": "application/json"})
        if not script_tag:
            self.logger.error("No __NEXT_DATA__ found on the page.")
            return {}

        # Load JSON data from the script tag
        next_data = json.loads(script_tag.string)
        props = next_data.get("props", {})

        # Extract the listing details from the JSON structure
        listing = props.get("pageProps", {}).get("listingDetails", {})
        if "suggestedListingResult" in listing:
            del listing["suggestedListingResult"]

        # Build a new dictionary with selected fields
        extracted = {
            "dutch_title": listing.get("dutchTitle"),
            "english_title": listing.get("englishTitle"),
            "dutch_desc": listing.get("dutchDescription"),
            "english_desc": listing.get("englishDescription"),
            "rental_price": listing.get("rentalPrice"),
            "deposit": listing.get("deposit"),
            "city_slug": listing.get("citySlug"),
            "street_slug": listing.get("streetSlug"),
            "house_number": listing.get("houseNumber"),
            "postal_code": listing.get("postalCode"),
            "latitude": listing.get("postalCodeLat"),
            "longitude": listing.get("postalCodeLong"),
            "landlord_name": listing.get("landlordDisplayName"),
            "landlord_user_id": listing.get("landlordUserId"),
            "landlord_member_since": listing.get("landlordMemberSince"),
            "landlord_last_logged": listing.get("landlordLastLoggedOn"),
            "energy_id": listing.get("energyId"),
            "internet_avail_id": listing.get("internetAvailableId"),
            "shower_id": listing.get("showerId"),
            "kitchen_id": listing.get("kitchenId"),
            "living_room_id": listing.get("livingRoomId"),
            "toilet_id": listing.get("toiletId"),
            "image_links": [f"https://resources.kamernet.nl/image/{uuid}" for uuid in listing.get("imageList", [])],
            "num_of_bedrooms": listing.get("numOfBedrooms"),
            "num_of_rooms": listing.get("numOfRooms"),
            "move_up_date": listing.get("moveUpDate"),
            "free_move_up_date": listing.get("freeMoveUpDate"),
            "external_url": listing.get("externalUrl"),
            "is_current_user_student_house": listing.get("isCurrentUserStudentHouseRoommate"),
            "internet_additional_costs": listing.get("internetAdditionalCosts"),
            "is_student_house_listing": listing.get("isStudentHouseListing"),
            "desired_tenant_languages": listing.get("desiredTenantLanguagesSpokenID"),
            "current_residents_languages": listing.get("currentResidentsLanguagesSpokenID"),
            "suitable_for_number_of_persons": listing.get("suitableForNumberOfPersons"),
            "create_date": listing.get("createDate"),
            "video_id_list": listing.get("videoIdList"),
            "is_moved_up": listing.get("isMovedUp"),
            "house_number_addition": listing.get("houseNumberAddition"),
            "candidate_pets_allowed": listing.get("candidatePetsAllowed"),
            "candidate_min_age": listing.get("candidateMinAgeId"),
            "candidate_max_age": listing.get("candidateMaxAgeId"),
            "candidate_smoking_allowed": listing.get("candidateSmokingAllowed"),
            "is_registration_allowed": listing.get("isRegistrationAllowed"),
            "publish_date": listing.get("publishDate"),
            "viewing_date": listing.get("viewingDate"),
            "candidate_status_id": listing.get("candidateStatusId"),
            "is_active": listing.get("isActive")
        }

        return extracted

    def scrape_listings(self, max_pages=None) -> pd.DataFrame:
        # Step 1: Collect raw listing data from all pages
        data_list = self.collect_all_pages(max_pages=max_pages)
        df = pd.DataFrame(data_list)

        # Step 2: Build the page_link column
        df['page_link'] = df.apply(self.build_page_link, axis=1)

        # Reset index to ensure a numeric, sequential index
        df = df.reset_index(drop=True)
        total_listings = len(df)

        # Step 3: For each row, call parse_listing_detail() using the page_link URL
        details = []
        for count, row in enumerate(df.itertuples(), start=1):
            url = row.page_link
            detail = self.parse_listing_detail(url)
            if detail:
                self.logger.info(f"Successfully fetched details for listing: {count}/{total_listings} - {url}")
            else:
                self.logger.warning(f"Failed to fetch details for listing: {count}/{total_listings} - {url}")
            details.append(detail)

        details_df = pd.DataFrame(details)

        # Step 4: Merge the details with the original DataFrame
        final_df = pd.concat([df, details_df], axis=1)
        return final_df




