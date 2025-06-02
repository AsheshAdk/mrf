import argparse
import requests

class Planet:
    def __init__(self, name, distance):
        self.name = name
        self.distance = distance

    def __repr__(self):
        return f"Planet : {self.name}, Distance from Sun : {self.distance} million km"

def pldata():
    api_url = "https://api.le-systeme-solaire.net/rest/bodies/"
    response = requests.get(api_url)
    data = response.json()
    planet_data = []
    for b in data["bodies"]:
        if b.get("isPlanet"):
            name = b.get("englishName")
            semi_major_axis = b.get("semimajorAxis")
            if semi_major_axis is not None:
                distance = semi_major_axis / 1_000_000
                planet_data.append(Planet(name, distance))
    return planet_data

def insertion_sort(planets, ascending=True):
    for i in range(1, len(planets)):
        current = planets[i]
        j = i-1
        if ascending:
            while j >= 0 and planets[j].distance > current.distance:
                planets[j+1] = planets[j]
                j -= 1
        else:
            while j >= 0 and planets[j].distance < current.distance:
                planets[j+1] = planets[j]
                j -= 1
        planets[j+1] = current
    return planets

def bubble_sort(planets, ascending=True):
    n = len(planets)
    for i in range(n):
        swapped = False
        for j in range(0, n-i-1):
            if ascending:
                if planets[j].distance > planets[j+1].distance:
                    planets[j], planets[j + 1] = planets[j+1], planets[j]
                    swapped = True
            else:
                if planets[j].distance < planets[j+1].distance:
                    planets[j], planets[j+1] = planets[j+1], planets[j]
                    swapped = True
        if not swapped:
            break
    return planets

def main():
    parser = argparse.ArgumentParser(description="Sort planets by distance from Sun using different algorithms")
    parser.add_argument('--sort',choices=['apiA', 'apiD'],required=True,
                        help="'--sort apiA for ascending or 'apiD'for descending")
    parser.add_argument('--algorithm', choices=['insertion','bubble'],required=True,
                        help="--algorithm insertion or bubble")
    args = parser.parse_args()

    planets = pldata() 
    asc = args.sort.endswith('A')
    
    if args.algorithm == "insertion":
        print("Sorting using insertion algorithm.")
        sorted_planets = insertion_sort(planets, asc)
    else:
        print("Sorting using bubble algorithm.")
        sorted_planets = bubble_sort(planets, asc)
    
    for planet in sorted_planets:
        print(planet)

if __name__=='__main__':
    main()