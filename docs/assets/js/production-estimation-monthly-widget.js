(function () {
$.widget('ed.productionestimationmonthly', {
    options: {
    },

    _plotlyFigureConfig: {
        "responsive": true
    },
    _plotlyFigureLayout: {
        "template": {
            "data": {
                "histogram2dcontour":[{"type":"histogram2dcontour","colorbar":{"outlinewidth":0,"ticks":""},"colorscale":[[0.0,"#0d0887"],[0.1111111111111111,"#46039f"],[0.2222222222222222,"#7201a8"],[0.3333333333333333,"#9c179e"],[0.4444444444444444,"#bd3786"],[0.5555555555555556,"#d8576b"],[0.6666666666666666,"#ed7953"],[0.7777777777777778,"#fb9f3a"],[0.8888888888888888,"#fdca26"],[1.0,"#f0f921"]]}],
                "choropleth":[{"type":"choropleth","colorbar":{"outlinewidth":0,"ticks":""}}],
                "histogram2d":[{"type":"histogram2d","colorbar":{"outlinewidth":0,"ticks":""},"colorscale":[[0.0,"#0d0887"],[0.1111111111111111,"#46039f"],[0.2222222222222222,"#7201a8"],[0.3333333333333333,"#9c179e"],[0.4444444444444444,"#bd3786"],[0.5555555555555556,"#d8576b"],[0.6666666666666666,"#ed7953"],[0.7777777777777778,"#fb9f3a"],[0.8888888888888888,"#fdca26"],[1.0,"#f0f921"]]}],
                "heatmap":[{"type":"heatmap","colorbar":{"outlinewidth":0,"ticks":""},"colorscale":[[0.0,"#0d0887"],[0.1111111111111111,"#46039f"],[0.2222222222222222,"#7201a8"],[0.3333333333333333,"#9c179e"],[0.4444444444444444,"#bd3786"],[0.5555555555555556,"#d8576b"],[0.6666666666666666,"#ed7953"],[0.7777777777777778,"#fb9f3a"],[0.8888888888888888,"#fdca26"],[1.0,"#f0f921"]]}],
                "heatmapgl":[{"type":"heatmapgl","colorbar":{"outlinewidth":0,"ticks":""},"colorscale":[[0.0,"#0d0887"],[0.1111111111111111,"#46039f"],[0.2222222222222222,"#7201a8"],[0.3333333333333333,"#9c179e"],[0.4444444444444444,"#bd3786"],[0.5555555555555556,"#d8576b"],[0.6666666666666666,"#ed7953"],[0.7777777777777778,"#fb9f3a"],[0.8888888888888888,"#fdca26"],[1.0,"#f0f921"]]}],
                "contourcarpet":[{"type":"contourcarpet","colorbar":{"outlinewidth":0,"ticks":""}}],
                "contour":[{"type":"contour","colorbar":{"outlinewidth":0,"ticks":""},"colorscale":[[0.0,"#0d0887"],[0.1111111111111111,"#46039f"],[0.2222222222222222,"#7201a8"],[0.3333333333333333,"#9c179e"],[0.4444444444444444,"#bd3786"],[0.5555555555555556,"#d8576b"],[0.6666666666666666,"#ed7953"],[0.7777777777777778,"#fb9f3a"],[0.8888888888888888,"#fdca26"],[1.0,"#f0f921"]]}],
                "surface":[{"type":"surface","colorbar":{"outlinewidth":0,"ticks":""},"colorscale":[[0.0,"#0d0887"],[0.1111111111111111,"#46039f"],[0.2222222222222222,"#7201a8"],[0.3333333333333333,"#9c179e"],[0.4444444444444444,"#bd3786"],[0.5555555555555556,"#d8576b"],[0.6666666666666666,"#ed7953"],[0.7777777777777778,"#fb9f3a"],[0.8888888888888888,"#fdca26"],[1.0,"#f0f921"]]}],
                "mesh3d":[{"type":"mesh3d","colorbar":{"outlinewidth":0,"ticks":""}}],
                "scatter":[{"fillpattern":{"fillmode":"overlay","size":10,"solidity":0.2},"type":"scatter"}],
                "parcoords":[{"type":"parcoords","line":{"colorbar":{"outlinewidth":0,"ticks":""}}}],
                "scatterpolargl":[{"type":"scatterpolargl","marker":{"colorbar":{"outlinewidth":0,"ticks":""}}}],
                "bar":[{"error_x":{"color":"#2a3f5f"},"error_y":{"color":"#2a3f5f"},"marker":{"line":{"color":"#E5ECF6","width":0.5},"pattern":{"fillmode":"overlay","size":10,"solidity":0.2}},"type":"bar"}],
                "scattergeo":[{"type":"scattergeo","marker":{"colorbar":{"outlinewidth":0,"ticks":""}}}],
                "scatterpolar":[{"type":"scatterpolar","marker":{"colorbar":{"outlinewidth":0,"ticks":""}}}],
                "histogram":[{"marker":{"pattern":{"fillmode":"overlay","size":10,"solidity":0.2}},"type":"histogram"}],
                "scattergl":[{"type":"scattergl","marker":{"colorbar":{"outlinewidth":0,"ticks":""}}}],
                "scatter3d":[{"type":"scatter3d","line":{"colorbar":{"outlinewidth":0,"ticks":""}},"marker":{"colorbar":{"outlinewidth":0,"ticks":""}}}],
                "scattermapbox":[{"type":"scattermapbox","marker":{"colorbar":{"outlinewidth":0,"ticks":""}}}],
                "scatterternary":[{"type":"scatterternary","marker":{"colorbar":{"outlinewidth":0,"ticks":""}}}],
                "scattercarpet":[{"type":"scattercarpet","marker":{"colorbar":{"outlinewidth":0,"ticks":""}}}],
                "carpet":[{"aaxis":{"endlinecolor":"#2a3f5f","gridcolor":"white","linecolor":"white","minorgridcolor":"white","startlinecolor":"#2a3f5f"},"baxis":{"endlinecolor":"#2a3f5f","gridcolor":"white","linecolor":"white","minorgridcolor":"white","startlinecolor":"#2a3f5f"},"type":"carpet"}],
                "table":[{"cells":{"fill":{"color":"#EBF0F8"},"line":{"color":"white"}},"header":{"fill":{"color":"#C8D4E3"},"line":{"color":"white"}},"type":"table"}],
                "barpolar":[{"marker":{"line":{"color":"#E5ECF6","width":0.5},"pattern":{"fillmode":"overlay","size":10,"solidity":0.2}},"type":"barpolar"}],
                "pie":[{"automargin":true,"type":"pie"}]
            },
            "layout": {
                "autotypenumbers":"strict",
                "colorway": ['#636efa', '#EF553B', '#00cc96', '#ab63fa', '#FFA15A', '#19d3f3', '#FF6692', '#B6E880', '#FF97FF', '#FECB52'],
                "font":{"color":"#2a3f5f"},
                "hovermode":"closest",
                "hoverlabel":{"align":"left"},
                "paper_bgcolor":"white",
                "plot_bgcolor":"#E5ECF6",
                "polar": {
                    "bgcolor":"#E5ECF6",
                    "angularaxis":{"gridcolor":"white","linecolor":"white","ticks":""},
                    "radialaxis":{"gridcolor":"white","linecolor":"white","ticks":""}
                },
                "ternary": {
                    "bgcolor":"#E5ECF6",
                    "aaxis":{"gridcolor":"white","linecolor":"white","ticks":""},
                    "baxis":{"gridcolor":"white","linecolor":"white","ticks":""},
                    "caxis":{"gridcolor":"white","linecolor":"white","ticks":""}
                },
                "coloraxis": {
                    "colorbar":{"outlinewidth":0,"ticks":""}
                },
                "colorscale": {
                    "sequential":[[0.0,"#0d0887"],[0.1111111111111111,"#46039f"],[0.2222222222222222,"#7201a8"],[0.3333333333333333,"#9c179e"],[0.4444444444444444,"#bd3786"],[0.5555555555555556,"#d8576b"],[0.6666666666666666,"#ed7953"],[0.7777777777777778,"#fb9f3a"],[0.8888888888888888,"#fdca26"],[1.0,"#f0f921"]],
                    "sequentialminus":[[0.0,"#0d0887"],[0.1111111111111111,"#46039f"],[0.2222222222222222,"#7201a8"],[0.3333333333333333,"#9c179e"],[0.4444444444444444,"#bd3786"],[0.5555555555555556,"#d8576b"],[0.6666666666666666,"#ed7953"],[0.7777777777777778,"#fb9f3a"],[0.8888888888888888,"#fdca26"],[1.0,"#f0f921"]],
                    "diverging":[[0,"#8e0152"],[0.1,"#c51b7d"],[0.2,"#de77ae"],[0.3,"#f1b6da"],[0.4,"#fde0ef"],[0.5,"#f7f7f7"],[0.6,"#e6f5d0"],[0.7,"#b8e186"],[0.8,"#7fbc41"],[0.9,"#4d9221"],[1,"#276419"]]
                },
                "xaxis": {
                    "gridcolor": "white",
                    "linecolor": "white",
                    "ticks": "",
                    "title": {
                        "standoff": 15
                    },
                    "zerolinecolor": "white",
                    "automargin": true,
                    "zerolinewidth": 2
                },
                "yaxis": {
                    "gridcolor": "white",
                    "linecolor": "white",
                    "ticks": "",
                    "title": {
                        "standoff": 15
                    },
                    "zerolinecolor": "white",
                    "automargin": true,
                    "zerolinewidth": 2
                },
                "scene":{
                    "xaxis":{"backgroundcolor":"#E5ECF6","gridcolor":"white","linecolor":"white","showbackground":true,"ticks":"","zerolinecolor":"white","gridwidth":2},
                    "yaxis":{"backgroundcolor":"#E5ECF6","gridcolor":"white","linecolor":"white","showbackground":true,"ticks":"","zerolinecolor":"white","gridwidth":2},
                    "zaxis":{"backgroundcolor":"#E5ECF6","gridcolor":"white","linecolor":"white","showbackground":true,"ticks":"","zerolinecolor":"white","gridwidth":2}
                },
                "shapedefaults": {
                    "line":{"color":"#2a3f5f"}
                },
                "annotationdefaults":{"arrowcolor":"#2a3f5f","arrowhead":0,"arrowwidth":1},
                "geo":{"bgcolor":"white","landcolor":"#E5ECF6","subunitcolor":"white","showland":true,"showlakes":true,"lakecolor":"white"},
                "title":{"x":0.05},
                "mapbox":{"style":"light"}
            }
        },
        "xaxis": {
            "tickangle":90
        },
        "legend": {
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "right",
            "x": 1
        },
        "title": {
            "text": "Producción estimada mensual"
        },
        "barmode":"relative"
    },
    _monthLiterals: {
        short: ['ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN', 'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC'],
        long: ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
    },
    _dataSeries: [{
        column: 'month_selfsupply_kwh',
        label: 'Autoconsumo',
        color: '#636efa'
    }, {
        column: 'month_finalconsumption_kwh',
        label: 'Consumo',
        color: '#EF553B'
    }, {
        column: 'month_exceeding_kwh',
        label: 'Excedente',
        color: '#00cc96'
    }],

    _configurations: [],

    _create: function () {
        window.PLOTLYENV=window.PLOTLYENV || {};
        this.DataFrame = dfjs.DataFrame;

        console.log(`created widget productionEstimationMontly on ${this.element.attr('id')}`);
    },
    _setOption: function (key, value) {
        this.options[key] = value;
    },

    _getPlotlyFigureLayout: function() {
        return JSON.parse(JSON.stringify(this._plotlyFigureLayout));
    },
    _getPlotlyData: function() {
        let self = this;

        dataLength = this._configurations.length;

        let plotlyData;

        if (dataLength == 1) {
            plotlyData = this._dataSeries.map(function(item) {
                return {
                    'name': item.label,
                    'x': self._monthLiterals.long,
                    'y': self._configurations[0].data[item.column],
                    'marker': {
                        'color': item.color
                    },
                    'type': 'bar',
                }
            });
        } else {
            let x_m = [];
            let x_label = [];

            this._monthLiterals.short.forEach(function(m_lit) {
                x_m = x_m.concat(Array(dataLength).fill(m_lit));
                self._configurations.forEach(function(data) {
                    x_label.push(data.name);
                })
            });

            plotlyData = this._dataSeries.map(function(item) {
                let totalArrayLength = 0;
                let y_m = [];

                self._configurations.forEach(function(rawData) {
                    totalArrayLength += rawData.data[item.column].length;
                });

                let run = 0;

                while (run < totalArrayLength) {
                    let dataIndex = Math.floor(run / dataLength);
                    let dataPos = run % dataLength;

                    y_m.push(self._configurations[dataPos].data[item.column][dataIndex]);

                    run++;
                }

                return {
                    'name': item.label,
                    'x': [x_m, x_label],
                    'y': y_m,
                    'marker': {
                        'color': item.color
                    },
                    'type': 'bar',
                }
            });
        }

        return plotlyData;
    },

    _configurationExits: function(configurationId) {
        return this._configurations.some(function(configuration) {
            return configuration.id === configurationId;
        });
    },
    _storeConfiguration: function(configuration) {
        this._configurations.push(configuration);
    },

    _refreshFigure: function() {
        Plotly.newPlot(this.element.attr('id'), this._getPlotlyData(), this._getPlotlyFigureLayout(), this._plotlyFigureConfig);
    },

    addConfiguration: function(rawConfiguration) {
        let self = this;

        let rawConfigurationList;

        if (Array.isArray(rawConfiguration)) {
            rawConfigurationList = rawConfiguration;
        } else {
            rawConfigurationList = [rawConfiguration];
        }

        rawConfigurationList.forEach(function(rc) {
            if (!self._configurationExits(rc.id)) {
                configuration = {
                    id: rc.id,
                    name: rc.name,
                    data: {}
                }

                let df = new self.DataFrame(rc.dataframe_m.data, rc.dataframe_m.columns);

                self._dataSeries.forEach(function(ds) {
                    configuration['data'][ds.column] = df.sortBy('month').select(ds.column).toDict()[ds.column];
                });

                self._storeConfiguration(configuration);
            }
        });

        self._refreshFigure();
    },
    removeConfiguration: function(configurationId) {
        let index = this._configurations.findIndex(function(configuration) {
            return configuration.id === configurationId;
        });

        if (index > -1) {
            this._configurations.splice(index, 1);
            this._refreshFigure();
        }

        if (this._configurations.length == 0) {
            this.element.empty();
        }
    }
});
})();