from geopy.geocoders import Nominatim


def get_lat_long(city_name):
    """
    Get latitude and longitude of a city using Nominatim geocoder.

    Args:
    - city_name (str): Name of the city

    Returns:
    - tuple: (latitude, longitude) of the city
    """
    geolocator = Nominatim(user_agent="city_locator")
    location = geolocator.geocode(city_name)
    if location:
        return location.latitude, location.longitude
    else:
        return None, None


# Example usage
cities = ['Düsseldorf', 'Stuttgart', 'Frankfurt', 'Berlin', 'Hamburg', 'Munich', 'Cologne', 'Dortmund', 'Leipzig', 'Essen']

for city in cities:
    latitude, longitude = get_lat_long(city)
    print(f"{city}: Latitude = {latitude}, Longitude = {longitude}")
