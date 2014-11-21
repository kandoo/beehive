package beehive

import (
	"bytes"
	"net/http"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/gorilla/mux"
)

var (
	beesPagePath = "/bees"
	beesPage     = genPage("Bees", beesScript, beesStyle, "")
	beesStyle    = `
		.infobox {
			float: left;
			width: 200px;
			margin: 20px;
		}

		svg {
			float: left;
		}
	`
	beesScript = `
		$(document).ready(function() {
			$.ajax({
				url: '/api/v1/bees',
				context: document.body
			}).done(function(data) {
				drawBees(data);
			}).error(function() {
				$('body').append('cannot fetch data');
			});
		})

		var BEE_R = 20;
		var BEE_HEAD_R = 5;
		var BEE_TAIL_RX = 12;
		var BEE_TAIL_RY = 5;
		var BEE_WING_RX = 4;
		var BEE_WING_RY = 10;
		var PI = Math.PI;
		var MARGIN = 10*BEE_TAIL_RX;

		function drawBees(bees) {
			bees.sort(function(a, b) {
				if (a.hive < b.hive) {
					return -1;
				}
				if (a.hive > b.hive) {
					return 1;
				}
				return b.id - a.id;
			});

			apps = bees.map(function(b) {
				return b.app;
			});
			apps = $.unique(apps);

			if (apps.length <= 10) {
				var color = d3.scale.category10();
			} else if (apps.length <= 20) {
				var color = d3.scale.category20();
			} else {
				var color = scale.linear().domain([1, apps.length])
																	.range(['red', 'blue']);
			}

			var appColors = {}
			for (i in apps) {
				appColors[apps[i]] = color(i+1);
			}

			var len = bees.length;
			var rad = 2 * BEE_R * len / PI;
			var deg = 2 * PI / len;
			var center = rad + MARGIN/2;
			var svg = d3.select('body').append('svg')
																 .attr('width', center * 2)
																 .attr('height', center * 2);
			svg.append('circle')
				 .attr('cx', center)
				 .attr('cy', center)
				 .attr('r', rad)
				 .attr('fill', 'white');

			for (i in bees) {
				var b = bees[i];
				var c = appColors[b.app];
				var x = center + Math.sin(i * deg)*rad;
				var y = center + Math.cos(i * deg)*rad;
				drawBee(b, x, y, i*deg, c, svg)
			}

			matrix = {
				0: {
					3: 4,
					2: 1,
					12: 1,
					10:1,
					9: 1,
					7: 1
				}
			};

			rad -= BEE_HEAD_R*4;
			for (i in bees) {
				var x1 = center + Math.sin(i * deg)*rad;
				var y1 = center + Math.cos(i * deg)*rad;
				for (j in bees) {
					if (i == j) {
						continue
					}

					if (!matrix[i] || !matrix[i][j]) {
						continue
					}

					var x2 = center + Math.sin(j * deg)*rad;
					var y2 = center + Math.cos(j * deg)*rad;

					var lineData = [
						{'x': x1, 'y': y1},
						{'x': center, 'y': center},
						{'x': x2, 'y': y2}
					];
					var lineFunction = d3.svg.line()
																	 .x(function(d) { return d.x; })
																	 .y(function(d) { return d.y; })
																	 .interpolate('basis');
					var lineGraph = svg.append('path')
														 .attr('d', lineFunction(lineData))
														 .attr('stroke', 'gray')
														 .attr('stroke-width', matrix[i][j])
														 .attr('fill', 'none');
				}
			}
		}

		function drawBee(b, x, y, d, c, svg) {
			var g = svg.append('g');
			g.append('ellipse')
				.attr('cx', x + BEE_HEAD_R)
				.attr('cy', y)
				.attr('rx', BEE_TAIL_RX)
				.attr('ry', BEE_TAIL_RY)
				.attr('style', 'stroke: ' + c + ';')
				.attr('fill', 'white');
			g.append('ellipse')
				.attr('cx', x)
				.attr('cy', y + BEE_WING_RY )
				.attr('rx', BEE_WING_RX)
				.attr('ry', BEE_WING_RY)
				.attr('style', 'stroke: ' + c + ';')
				.attr('fill', 'white')
				.attr('transform', 'rotate(-30 ' + x + ' ' + y + ')');
			g.append('ellipse')
				.attr('cx', x)
				.attr('cy', y - BEE_WING_RY )
				.attr('rx', BEE_WING_RX)
				.attr('ry', BEE_WING_RY)
				.attr('style', 'stroke: ' + c + ';')
				.attr('fill', 'white')
				.attr('transform', 'rotate(30 ' + x + ' ' + y + ')');
			g.append('ellipse')
				.attr('cx', x)
				.attr('cy', y + BEE_WING_RY)
				.attr('rx', BEE_WING_RX)
				.attr('ry', BEE_WING_RY)
				.attr('style', 'stroke: ' + c + ';')
				.attr('fill', 'white');
			g.append('ellipse')
				.attr('cx', x)
				.attr('cy', y - BEE_WING_RY)
				.attr('rx', BEE_WING_RX)
				.attr('ry', BEE_WING_RY)
				.attr('style', 'stroke: ' + c + ';')
				.attr('fill', 'white');
			g.append('circle')
				.attr('cx', x - BEE_R + BEE_HEAD_R*2)
				.attr('cy', y)
				.attr('r', BEE_HEAD_R)
				.attr('style', 'stroke: ' + c + ';')
				.attr('fill', 'white');
			g.attr('transform', 'rotate(' + (90 - d*180/PI) + ' ' + x + ' ' + y +')');
			g.on('click', function() {
				showBeeInfo(b);
			});
		}

		function showBeeInfo(b) {
			if ($('.infobox').length == 0) {
				$('body').append('<div class="infobox"></div>');
			}
			var info = $('.infobox');
			info.empty();
			info.append('<div>ID: ' + b.id + '</div>');
			info.append('<div>App: ' + b.app + '</div>');
			info.append('<div>Hive: ' + b.hive + '</div>');
			info.append('<div>Detached: ' + (b.detached ? 'YES' : 'NO') + '</div>');
			if (b.colony.leader == b.id) {
				info.append('<div>Is Leader: YES</div>');
			} else {
				if (!b.colony.leader) {
					info.append('<div>Leader: None</div>');
				} else {
					info.append('<div>Leader:' + b.colony.leader + '</div>');
				}
			}
			if (b.colony.followers == null) {
				info.append('<div>Followers: None</div>');
			} else {
				info.append('<div>Followers: ' +  b.colony.followes.join(',') +
						'</div>');
			}
		}
	`
)

func genPage(title, script, style, body string) []byte {
	var b bytes.Buffer
	b.WriteString(`
	<!DOCTYPE HTML>
	<html>
		<head>
			<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
			<title>`)
	b.WriteString(title)
	b.WriteString(`
			</title>
			<style>
	`)
	b.WriteString(style)
	b.WriteString(`
			</style>
			<script>
	`)
	b.WriteString(jQuery)
	b.WriteString(d3JS)
	b.WriteString(`
			</script>
			<script>`)
	b.WriteString(script)
	b.WriteString(`
			</script>
		</head>
		<body>`)
	b.WriteString(body)
	b.WriteString(`
		</body>
	</html>
	`)
	return b.Bytes()
}

type webHandler struct {
	h *hive
}

func (h *webHandler) Install(r *mux.Router) {
	r.HandleFunc(beesPagePath, h.handleBees)
}

func (h *webHandler) handleBees(w http.ResponseWriter, r *http.Request) {
	w.Write(beesPage)
}
