(function () {
$.widget('ed.productionestimationyearly', {
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
            "title": {
                "font": {
                    "family": "Monospace"
                }
            }
        },
        "legend": {
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "right",
            "x": 1
        },
        "barmode":"relative"
    },
    _dataSeries: [{
        column: 'year_selfsupply_kwh',
        label: 'Autoconsumo',
        color: '#636efa',
        template: 'Configuración<br>%{x}<br>Autoconsumo: %{y:.3f} kWh<br>%{customdata[2]:.2f} % (T.Consumida: %{customdata[0]:.3f} kWh)<br>%{customdata[3]:.2f} % (T.Producida: %{customdata[1]:.3f} kWh)<extra></extra>',
        extra_data: ['year_consumption_kwh', 'year_production_kwh', 'pct_selfsupply_vs_consumption', 'pct_selfsupply_vs_production']
    }, {
        column: 'year_finalconsumption_kwh',
        label: 'Consumo',
        color: '#EF553B',
        template: 'Configuración<br>%{x}<br>Consumo: %{y} kWh<br>%{customdata[1]:.2f} % (T.Consumida: %{customdata[0]:.3f} kWh)<extra></extra>',
        extra_data: ['year_consumption_kwh', 'pct_finalconsumption_vs_consumption']
    }, {
        column: 'year_exceeding_kwh',
        label: 'Excedente',
        color: '#00cc96',
        template: 'Configuración<br>%{x}<br>Excedente: %{y} kWh<br>%{customdata[1]:.2f} % (T.Producida: %{customdata[0]:.3f} kWh)<extra></extra>',
        extra_data: ['year_production_kwh', 'pct_exceeding_vs_production']
    }],

    _configurations: [],

    _create: function () {
        window.PLOTLYENV=window.PLOTLYENV || {};
        this.DataFrame = dfjs.DataFrame;

        console.log(`created widget productionEstimationYearly on ${this.element.attr('id')}`);
    },
    _setOption: function (key, value) {
        this.options[key] = value;
    },

    _getPlotlyFigureLayout: function() {
        let layout = JSON.parse(JSON.stringify(this._plotlyFigureLayout));

        if (this._configurations.length > 3) {
            layout['xaxis'] = {
                'tickangle': 90
            }
        }

        return layout;
    },
    _getPlotlyData: function() {
        let self = this;

        let x = self._configurations.map(configuration => configuration.name);

        return this._dataSeries.map(item => {
            let y = self._configurations.map(configuration => configuration['data'][item.column]);

            let customData = [];

            if (item.extra_data) {
                this._configurations.forEach(configuration => {
                    customData.push(item.extra_data.map(extra_data => configuration['extra'][extra_data][0]));
                });
            }

            return {
                'name': item.label,
                'x': x,
                'y': y,
                'customdata': customData,
                'hovertemplate': item.template,
                'marker': {
                    'color': item.color
                },
                'type': 'bar',
            }
        });
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
        let elementId = this.element.attr('id');

        let titleId = `${elementId}-title`
        let $titleElement = $(`#${titleId}`);

        if ($titleElement.length === 0) {
            this.element.append($('<h2>', {id: titleId, text: 'Consumo estimado anual'}))
        }

        let figureId = `${elementId}-figure`
        let $figureElement = $(`#${figureId}`);

        if ($figureElement.length === 0) {
            this.element.append($('<div>', {id: figureId,  style: 'min-height:600px; width:100%;', class: 'plotly-graph-div'}));
        }

        Plotly.newPlot(figureId, this._getPlotlyData(), this._getPlotlyFigureLayout(), this._plotlyFigureConfig);
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
                    data: {},
                    extra: {}
                }

                let df = new self.DataFrame(rc.dataframe_y.data, rc.dataframe_y.columns);

                self._dataSeries.forEach(function(ds) {
                    configuration['data'][ds.column] = df.select(ds.column).toDict()[ds.column][0];
                });

                configuration['extra']['year_consumption_kwh'] = df.sortBy('month').select('year_consumption_kwh').toDict()['year_consumption_kwh'];
                configuration['extra']['year_production_kwh'] = df.sortBy('month').select('year_production_kwh').toDict()['year_production_kwh'];
                configuration['extra']['pct_selfsupply_vs_consumption'] = df.withColumn('aux', (row) => row.get('year_selfsupply_kwh') / row.get('year_consumption_kwh') * 100).sortBy('month').select('aux').toDict()['aux'];
                configuration['extra']['pct_selfsupply_vs_production'] = df.withColumn('aux', (row) => row.get('year_selfsupply_kwh') / row.get('year_production_kwh') * 100).sortBy('month').select('aux').toDict()['aux'];
                configuration['extra']['pct_finalconsumption_vs_consumption'] = df.withColumn('aux', (row) => row.get('year_finalconsumption_kwh') / row.get('year_consumption_kwh') * 100).sortBy('month').select('aux').toDict()['aux'];
                configuration['extra']['pct_exceeding_vs_production'] = df.withColumn('aux', (row) => row.get('year_exceeding_kwh') / row.get('year_production_kwh') * 100).sortBy('month').select('aux').toDict()['aux'];

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