package beehive

import (
	"bytes"
	"net/http"

	"github.com/kandoo/beehive/Godeps/_workspace/src/github.com/gorilla/mux"
)

type webPage struct {
	title  string
	url    string
	onMenu bool
	script string
	style  string
	body   string
	page   []byte
}

// handle must use a copy of p because this function is used as a closure in a
// loop.
func (p webPage) handle(w http.ResponseWriter, r *http.Request) {
	w.Write(p.page)
}

var (
	webPages = []webPage{
		{
			title:  "Status",
			url:    "/",
			onMenu: true,
			script: statusScript,
			style:  statusStyle,
		},
		{
			title:  "Bees",
			url:    "/bees",
			onMenu: true,
			script: beesScript,
			style:  beesStyle,
		},
		{
			title:  "Traffic Matrix",
			url:    "/matrix",
			onMenu: true,
			script: matrixScript,
			style:  matrixStyle,
		},
		{
			title:  "About",
			url:    "/about",
			onMenu: true,
			body:   aboutBody,
		},
	}

	bgColor = `#12191A`

	statusStyle = `
		.heading{
			font-size: 14pt;
			margin: 20px 0px 20px 10px;
		}

		.hive {
			margin: 20px;
		}

		.addr {
			color: #EEE;
		}
	`
	statusScript = `
		$(document).ready(function() {
			$.ajax({
				url: '/api/v1/state',
				context: document.body
			}).done(function(data) {
				writeState(data);
			}).error(function() {
				$('body').append('cannot fetch data');
			});
		});

		function writeState(state) {
			$('body').append('<div class="heading">' +
													state.peers.length + ' live hives:' +
												'</div>');
			for (var i in state.peers) {
				var p = state.peers[i];
				$('<div>', {
						'id': 'hive'+ p.id,
						'class': 'hive',
						'html': '<a href="http://' + p.addr + '" class="addr">Hive ' +
											p.id +
										'</a>'
					}).appendTo('body');
			}
		}
	`
	beesStyle = `
		@import url(http://fonts.googleapis.com/css?family=Ubuntu+Mono);

		.infobox {
			color: #EEE;
			float: left;
			font-family: 'Ubuntu Mono';
			width: 200px;
			margin: 20px;
		}

		svg {
			float: left;
		}
	`
	sortBees = `
		function sortBees(bees) {
			bees.sort(function(a, b) {
				if (a.hive < b.hive) {
					return -1;
				}
				if (a.hive > b.hive) {
					return 1;
				}
				return a.id - b.id;
			});
			return bees;
		}
	`
	generateMatrixColor = `
		function matrixValue(matrix, i, j) {
			return (matrix[i] && matrix[i][j] || 0) +
						 (matrix[j] && matrix[j][i] || 0);
		}

		function generateMatrixColor(matrix, baseColor) {
			var max = 0;
			for (var id1 in matrix) {
				for (var id2 in matrix[id1]) {
					max = Math.max(max, matrixValue(matrix, id1, id2));
				}
			}

			var colored = {}
			for (var id1 in matrix) {
				colored[id1] = {}
				for (var id2 in matrix[id1]) {
					var m = matrixValue(matrix, id1, id2);
					colored[id1][id2] = baseColor.brighter(6*m/max);
				}
			}
			return colored;
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
		}); ` +
		sortBees +
		generateMatrixColor + `
		var BEE_R = 20;
		var BEE_HEAD_R = 5;
		var BEE_TAIL_RX = 12;
		var BEE_TAIL_RY = 5;
		var BEE_WING_RX = 4;
		var BEE_WING_RY = 10;
		var PI = Math.PI;
		var MARGIN = 10 * BEE_TAIL_RX;

		function getCenter(len) {
			return getRadius(len) + MARGIN/2;
		}

		function getRadius(len) {
			return 2 * BEE_R * len / PI;
		}

		function getDegree(len) {
			return 2 * PI / len;
		}

		function drawBees(bees) {
			sortBees(bees);
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
			var center = getCenter(len);
			var rad = getRadius(len);
			var deg = getDegree(len);

			var svg = d3.select('body').append('svg')
																 .attr('width', center * 2)
																 .attr('height', center * 2);
			for (i in bees) {
				var b = bees[i];
				var c = appColors[b.app];
				var x = center + Math.sin(i * deg)*rad;
				var y = center + Math.cos(i * deg)*rad;
				drawBee(b, x, y, i*deg, c, svg)
			}

			$.ajax({
				url: '/apps/bh_collector/stats',
				context: document.body
			}).done(function(stats) {
				drawLinks(svg, bees, stats);
			}).error(function() {
				alert('cannot load stats');
			});
		}

		function drawLinks(svg, bees, matrix) {
			var len = bees.length;
			var center = getCenter(len);
			var rad = getRadius(len) - BEE_HEAD_R*4;
			var deg = getDegree(len);
			indices = {};
			for (var i in bees) {
				indices[bees[i].id] = +i;
			}

			var max = 0;
			for (var i in matrix) {
				for (var j in matrix[i]) {
					if (max < matrix[i][j]) {
						max = matrix[i][j];
					}
				}
			}

			var baseColor = d3.rgb(1, 1, 1);
			var colorized = generateMatrixColor(matrix, baseColor);
			for (var id1 in matrix) {
				var i = indices[id1];
				var x1 = center + Math.sin(i * deg)*rad;
				var y1 = center + Math.cos(i * deg)*rad;

				var row = matrix[id1];
				for (var id2 in row) {
					var j = indices[id2];
					var c = colorized[id1] && colorized[id1][id2] || null;
					if (!c) {
						continue;
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
														 .attr('stroke', c)
														 .attr('stroke-width', 2)
														 .attr('fill', 'none');
				}
			}
		}

		var bgColor = '` + bgColor + `';
		function drawBee(b, x, y, d, c, svg) {
			var g = svg.append('g');
			g.append('ellipse')
				.attr('cx', x + BEE_HEAD_R)
				.attr('cy', y)
				.attr('rx', BEE_TAIL_RX)
				.attr('ry', BEE_TAIL_RY)
				.attr('style', 'stroke: ' + c + ';')
				.attr('fill', bgColor);
			g.append('ellipse')
				.attr('cx', x)
				.attr('cy', y + BEE_WING_RY )
				.attr('rx', BEE_WING_RX)
				.attr('ry', BEE_WING_RY)
				.attr('style', 'stroke: ' + c + ';')
				.attr('fill', bgColor)
				.attr('transform', 'rotate(-30 ' + x + ' ' + y + ')');
			g.append('ellipse')
				.attr('cx', x)
				.attr('cy', y - BEE_WING_RY )
				.attr('rx', BEE_WING_RX)
				.attr('ry', BEE_WING_RY)
				.attr('style', 'stroke: ' + c + ';')
				.attr('fill', bgColor)
				.attr('transform', 'rotate(30 ' + x + ' ' + y + ')');
			g.append('ellipse')
				.attr('cx', x)
				.attr('cy', y + BEE_WING_RY)
				.attr('rx', BEE_WING_RX)
				.attr('ry', BEE_WING_RY)
				.attr('style', 'stroke: ' + c + ';')
				.attr('fill', bgColor);
			g.append('ellipse')
				.attr('cx', x)
				.attr('cy', y - BEE_WING_RY)
				.attr('rx', BEE_WING_RX)
				.attr('ry', BEE_WING_RY)
				.attr('style', 'stroke: ' + c + ';')
				.attr('fill', bgColor);
			g.append('circle')
				.attr('cx', x - BEE_R + BEE_HEAD_R*2)
				.attr('cy', y)
				.attr('r', BEE_HEAD_R)
				.attr('style', 'stroke: ' + c + ';')
				.attr('fill', bgColor);
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

	matrixStyle  = ""
	matrixScript = `
		$(document).ready(function() {
			$.ajax({
				url: '/api/v1/bees',
				context: document.body
			}).done(function(data) {
				drawMatrix(data);
			}).error(function() {
				$('body').append('cannot fetch data');
			});
		}); ` +
		sortBees +
		generateMatrixColor + `
		var CELL_WIDTH = 20;
		var MARGIN = 50;

		function drawMatrix(bees) {
			sortBees(bees);
			var svg = d3.select('body').append('svg')
																 .attr('width', bees.length * 30)
																 .attr('height', bees.length * 30);

			var offset = function(i) { return (i + 1)*CELL_WIDTH + MARGIN; };
			svg.selectAll('.beeXLables')
				.data(bees)
				.enter().append('text')
				.text(function (d) { return d.hive + "/" + d.id; })
				.attr('x', function(d, i) { return offset(i); })
				.attr('y', MARGIN)
				.attr('fill', '#EEE')
				.attr('transform', function(d, i) {
					return 'rotate(270 ' + offset(i) + ',' + MARGIN + ')'; }
				);
			svg.selectAll('.beeYLables')
				.data(bees)
				.enter().append('text')
				.text(function (d) { return d.hive + "/" + d.id; })
				.attr('x', MARGIN)
				.attr('y', function(d, i) { return offset(i); })
				.attr('fill', '#EEE')
				.style('text-anchor', 'end');

			$.ajax({
				url: '/apps/bh_collector/stats',
				context: document.body
			}).done(function(matrix) {
				drawCells(svg, bees, matrix);
			}).error(function() {
				alert('cannot load stats');
			});
		}

		var baseColor = d3.rgb(1, 1, 1);
		function drawCells(svg, bees, matrix) {
			var colorized = generateMatrixColor(matrix, baseColor);
			for (var i = 0; i < bees.length; i++) {
				var bx = bees[i];
				for (var j = 0; j < bees.length; j++) {
					var by = bees[j];
					var c = colorized[bx.id] && colorized[bx.id][by.id] || null;
					if (!c) {
						continue;
					}
					var rect = svg.append('rect')
												.attr('x', 55 + i * CELL_WIDTH)
												.attr('y', 55 + j * CELL_WIDTH)
												.attr('width', CELL_WIDTH)
												.attr('height', CELL_WIDTH);
					rect.attr('fill', c);
				}
			}
		}
	`
	aboutBody = `<div style="margin: 20px;">
								 Beehive Distributed Programming Framework
							 </div>`
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
				body {
					background: ` + bgColor + `;
					color: #EEE;
					font-family: 'Ubuntu Mono';
					margin: 0;
					padding: 0;
				}

				.bar {
					background: #345;
					padding: 1em 1em 1em 1em;
					vertical-align: middle;
					box-shadow: 0px 5px 10px #000;
				}

				.bar a {
					margin: 0em 1em 0em 1em;
					color: #999;
					text-decoration: none;
				}

				.bar a:hover {
					color: #FFF;
				}

				.bar .active {
					color: #FFF;
				}
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
		<body>
			<div class="bar">
		`)
	for _, p := range webPages {
		var c string
		if p.title == title {
			c = "active"
		}
		b.WriteString(`
				/// <a class="` + c + `" href="` + p.url + `">` + p.title + `</a>
		`)
	}
	b.WriteString(`
			</div>
		`)
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

func (h *webHandler) install(r *mux.Router) {
	for _, p := range webPages {
		p.page = genPage(p.title, p.script, p.style, p.body)
		r.HandleFunc(p.url, p.handle)
	}
}
