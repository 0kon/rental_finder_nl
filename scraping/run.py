from kammernet_scraper import KammernetScraper

import logging


# Configure logging
logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    # Instantiate the scraper for netherlands
    scraper = KammernetScraper()

    # Call scrape_listings() to fetch all listings as a DataFrame
    df = scraper.scrape_listings()

    # Save DataFrame to CSV
    csv_path = "kammernet_eindhoven_listings.csv"
    df.to_csv(csv_path, index=False)
    print(f"Saved {len(df)} listings to {csv_path}")

    # Preview the DataFrame
    print(df.head())
