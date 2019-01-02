from geopy.geocoders import Nominatim
import csv


def get_geocode(city_names):
    geolocator = Nominatim(user_agent="ipc-city")
    city_geocode = {}
    latitude = {}
    longitude = {}
    not_in = {}
    with open('../conf/city_with_country_geocode.csv', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            city_geocode[row[0]] = (float(row[1]), float(row[2]))

    for city in city_names:
        if city.upper() in city_geocode:
            latitude[city.upper()] = city_geocode[city.upper()][0]
            longitude[city.upper()] = city_geocode[city.upper()][1]
        else:
            try:
                print('存在记录中没有的城市，正调用API获取经纬度：{}'.format(city.upper()))
                location = geolocator.geocode(city.upper())
                try:
                    latitude[city.upper()] = float(location.latitude)
                    longitude[city.upper()] = float(location.longitude)
                    not_in[city.upper()] = (float(location.latitude), float(location.longitude))
                except AttributeError as e:
                    print(e)
                    latitude[city.upper()] = float(999)
                    longitude[city.upper()] = float(999)
                    not_in[city.upper()] = (float(999), float(999))
            except Exception as e:
                print('中途发生错误（可能是被发现滥用），正在保存现有结果', e)

                with open('../conf/city_with_country_geocode.csv', encoding='utf-8', mode='a', newline='') as file:
                    writer = csv.writer(file, quotechar='"')
                    for k, v in not_in.items():
                        writer.writerow([k, v[0], v[1]])
                not_in.clear()

    if len(not_in) > 0:
        with open('../conf/city_with_country_geocode.csv', encoding='utf-8', mode='a', newline='') as file:
            writer = csv.writer(file, quotechar='"')
            for k, v in not_in.items():
                writer.writerow([k, v[0], v[1]])

    return latitude, longitude


# if __name__ == '__main__':
    # cities = []
    # with open('../conf/city_with_country.txt', encoding='utf-8') as file:
    #     reader = csv.reader(file)
    #     for city in reader:
    #         cities.append(city[0])
    # print(len(cities))
    # print(cities[0])
    #
    # get_geocode(cities)
    # print(get_geocode(['MüNCHEN,DE']))
