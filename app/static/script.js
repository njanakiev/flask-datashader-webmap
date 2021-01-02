var minZoom = 0;
var maxZoom = 16;

var osm = L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {
	attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors', 
    minZoom: minZoom, 
    maxZoom: maxZoom,
    opacity: 0.6
});

var pillow_lyr = L.tileLayer('./pillow_tiles/{z}/{x}/{y}.png', {
	opacity: 0.8, 
	attribution: "", 
	minZoom: minZoom, 
	maxZoom: maxZoom
});

var lyr = L.tileLayer('./tiles/{z}/{x}/{y}.png', {
	opacity: 0.8, 
	attribution: "", 
	minZoom: minZoom, 
	maxZoom: maxZoom
});

// Source: https://png-pixel.com/
var white = L.tileLayer("data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=", {
	minZoom: minZoom, 
	maxZoom: maxZoom
});

var map = L.map('map', {
    center: [9.827287488020211, 2.4158453015843406e-13],
    zoom: 7,
    minZoom: minZoom,
    maxZoom: maxZoom,
    layers: [osm]
});

var basemaps = {
	"OpenStreetMap": osm, 
	"White Background": white,
	"Tile Grid": pillow_lyr
}
var overlaymaps = {"GPS Tracks": lyr}

L.control.layers(basemaps, overlaymaps, {collapsed: false}).addTo(map);

map.fitBounds([[-64.45438291583403, 170.00000000000048], [84.10895789187445, -170.0]]);
