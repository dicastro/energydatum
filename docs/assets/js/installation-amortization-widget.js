(function () {
$.widget('ed.installationamortization', {
    options: {
    },

    _colors: ['#636efa', '#EF553B', '#00cc96', '#ab63fa', '#FFA15A', '#19d3f3', '#FF6692', '#B6E880', '#FF97FF', '#FECB52'],

    _config: {
        installationLifeYears: 20,
        energyPriceYearInflation: 0
    },

    _plotlyFigureConfig: {"responsive": true},
    _plotlyFigureLayout: {
        "template": {
            "data": {
                "histogram2dcontour": [{"type":"histogram2dcontour","colorbar":{"outlinewidth":0,"ticks":""},"colorscale":[[0.0,"#0d0887"],[0.1111111111111111,"#46039f"],[0.2222222222222222,"#7201a8"],[0.3333333333333333,"#9c179e"],[0.4444444444444444,"#bd3786"],[0.5555555555555556,"#d8576b"],[0.6666666666666666,"#ed7953"],[0.7777777777777778,"#fb9f3a"],[0.8888888888888888,"#fdca26"],[1.0,"#f0f921"]]}],
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
                    "gridcolor":"white",
                    "linecolor":"white",
                    "ticks":"",
                    "title":{"standoff":15},
                    "zerolinecolor":"white",
                    "automargin":true,
                    "zerolinewidth":2
                },
                "yaxis": {
                    "gridcolor":"white",
                    "linecolor":"white",
                    "ticks":"",
                    "title":{"standoff":15},
                    "zerolinecolor":"white",
                    "automargin":true,
                    "zerolinewidth":2
                },
                "scene": {
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
            "anchor": "y",
            "domain": [0.0, 1.0],
            "title": {
                "text": "Año"
            }
        },
        "yaxis": {
            "anchor": "x",
            "domain": [0.0, 1.0],
            "title": {
                "text": "Beneficio (€)"
            }
        },
        "legend": {
            "title": {
                "text": "Instalación"
            },
            "tracegroupgap":0
        },
        "margin":{"t":60}
    },
    _getPlotlyFigureData(amortizationPlan) {
        return {
            'hovertemplate': `Instalación<br>${amortizationPlan.title}<br>Año=%{x}<br>Beneficio=%{y}<br>Ahorro año=%{customdata[0]}<extra></extra>`,
            'legendgroup': amortizationPlan.title,
            'line': {
                'color': amortizationPlan.color,
                'dash': 'solid'
            },
            'marker': {
                'symbol': 'circle'
            },
            'mode': 'lines',
            'name': amortizationPlan.title,
            'customdata': amortizationPlan.extra,
            'orientation': 'v',
            'showlegend': true,
            'x': amortizationPlan.x,
            'xaxis': 'x',
            'y': amortizationPlan.y,
            'yaxis': 'y',
            'type': 'scatter'
        }
    },

    _components: [],
    _configurations: [],
    _amortizationPlans: [],

    _create: function () {
        console.log(`created widget installationAmortization on ${this.element.attr('id')}`);
    },
    _destroy: function () {
        this._components.forEach(component => component.remove());
        this._components = [];
        this._configurations = [];
        this._amortizationPlans = [];

        this.element.empty();
    },

    _isInteger: function(str) {
        if (typeof str != "string") {
            return false
        }

        return !isNaN(str) && !isNaN(parseInt(str))
    },
    _bindEvents: function () {
        this.element.on('keypress', '.onlynumbers', event => {
            return event.charCode >= 48 && event.charCode <= 57
        });

        this._components['config'].on('change', 'input', event => {
            let $input = $(event.currentTarget);
            let fieldName = $input.attr('name');
            let fieldValue = $input.val();

            this._config[fieldName] = parseInt(fieldValue);

            this._updateAmortizationPlans();
            this._refreshFigure();
        });

        this._components['form'].on('change', 'input.editable-number', event => {
            let $input = $(event.currentTarget);
            let configurationId = $input.data('configuration-id');
            let fieldName = $input.attr('name');
            let fieldValue = $input.val();

            let configuration = this._configurations.find(configuration => configuration.id == configurationId);

            if (this._isInteger(fieldValue) && parseFloat(fieldValue) > 0) {
                configuration[fieldName] = parseFloat(fieldValue);

                $input.parents('.field').removeClass('error');
            } else {
                delete configuration[fieldName];

                $input.parents('.field').addClass('error');
            }

            if (this._isValidConfiguration(configuration)) {
                this._updateAmortizationPlan(configuration);
            } else {
                this._removeAmortizationPlan(configuration.id);
            }

            this._refreshFigure();
        });

        this._components['form'].on('click', '.remove-configuration', event => {
            event.preventDefault();
            let $element = $(event.currentTarget);
            let configurationId = $element.data('configuration-id');

            this._removeConfiguration(configurationId);
            let removedAmortizationPlan = this._removeAmortizationPlan(configurationId);

            if (removedAmortizationPlan) {
                this._refreshFigure();
            }

            $element.parents('.fields').remove();

            if (this._configurations.length === 0) {
                this._components['form'].append($('<div class="ui segment nocontent"><p>No se ha seleccionado ninguna configuración</p></div>'));
            }
        });
    },
    _getConfigurationId: function(configuration) {
        let taxId = configuration.with_tax ? 'IT' + configuration.tax : 'ET';

        let buyId = configuration.buy.toUpperCase()
            .replace(/ /g, '')
            .replace(/á/g, 'a')
            .replace(/é/g, 'e')
            .replace(/í/g, 'i')
            .replace(/ó/g, 'ó')
            .replace(/ú/g, 'u')
            .replace(/ñ/g, 'n');

        let sellId = configuration.sell.toUpperCase()
            .replace(/ /g, '')
            .replace(/á/g, 'a')
            .replace(/é/g, 'e')
            .replace(/í/g, 'i')
            .replace(/ó/g, 'ó')
            .replace(/ú/g, 'u')
            .replace(/ñ/g, 'n');

        return `${configuration.id}_B${buyId}_S${sellId}_${taxId}`;
    },
    _getTaxLabel: function(configuration) {
        return configuration.with_tax ? configuration.tax + '%' : 'SIN';
    },
    _configurationExists: function(configurationId) {
        return this._configurations.some(function(configuration) {
            return configuration.id === configurationId;
        });
    },
    _removeConfiguration: function(configurationId) {
        let index = this._configurations.findIndex(configuration => configuration.id == configurationId);

        if (index !== -1) {
            this._configurations.splice(index, 1);
        }
    },
    _isValidConfiguration: function(configuration) {
        return configuration.price !== undefined && configuration.price > 0 && configuration.ibidto !== undefined;
    },
    _getValidConfigurations: function() {
        return this._configurations.filter(configuration => this._isValidConfiguration(configuration));
    },
    _preselectOption: function(options, preselectedOption) {
        if (preselectedOption !== undefined) {
            options.forEach(option => {
                if (option.value === preselectedOption) {
                    option.selected = true;
                }
            });
        }
    },
    _getIbiDtoYears: function(preselectedOption) {
        let options = [
            { name: '0', value: 0 },
            { name: '1', value: 1 },
            { name: '2', value: 2 },
            { name: '3', value: 3 },
            { name: '4', value: 4 },
            { name: '5', value: 5 },
            { name: '6', value: 6 },
            { name: '7', value: 7 },
            { name: '8', value: 8 },
            { name: '9', value: 9 },
            { name: '10', value: 10 }
        ];

        this._preselectOption(options, preselectedOption);

        return options;
    },
    _getInstallationLifeYearsValues: function(preselectedOption) {
        let options = [
            { name: '20', value: 20 },
            { name: '25', value: 25 },
            { name: '30', value: 30 }
        ];

        this._preselectOption(options, preselectedOption);

        return options;
    },
    _getEnergyPriceYearInflationValues: function(preselectedOption) {
        let options = [
            { name: '0', value: 0 },
            { name: '1', value: 1 },
            { name: '2', value: 2 },
            { name: '3', value: 3 },
            { name: '4', value: 4 },
            { name: '5', value: 5 },
            { name: '6', value: 6 },
            { name: '7', value: 7 },
            { name: '8', value: 8 },
            { name: '9', value: 9 },
            { name: '10', value: 10 }
        ];

        this._preselectOption(options, preselectedOption);

        return options;
    },
    _updateAmortizationPlan: function(configuration) {
        let color;

        if (this._amortizationPlans[configuration.id]) {
            color = this._amortizationPlans[configuration.id].color;
        } else {
            color = this._colors.shift();
        }

        let start = -configuration.price;
        let yearSaving = configuration.yearSaving;

        let xaxis = [];
        let yaxis = [];
        let extra = [];

        let extraValue = '0';

        for (let y = 0; y <= this._config.installationLifeYears; y++) {
            xaxis.push(y.toString());
            yaxis.push(Math.round(start * 100) / 100);
            extra.push([extraValue]);

            extraValue = `${yearSaving}`;
            let yearSaved = yearSaving;

            start += yearSaving;
            yearSaving = Math.round(yearSaving * (1 + (this._config.energyPriceYearInflation / 100)) * 100) / 100;

            if (configuration.ibidto > 0 && configuration.ibiyears > 0 && y < configuration.ibiyears) {
                start += configuration.ibidto;
                yearSaved += configuration.ibidto;

                extraValue += ` + ${configuration.ibidto} = ${Math.round(yearSaved * 100) / 100}`;
            }
        }

        this._amortizationPlans[configuration.id] = {
            x: xaxis,
            y: yaxis,
            color: color,
            title: `  ${configuration.name}<br>  C: ${configuration.buy}<br>  V: ${configuration.sell}<br>  (IVA: ${configuration.tax})`,
            extra: extra
        }
    },
    _updateAmortizationPlans: function(configuration) {
        let self = this;

        this._configurations
            .filter(configuration => self._isValidConfiguration(configuration))
            .forEach(configuration => {
                this._updateAmortizationPlan(configuration);
            });
    },
    _removeAmortizationPlan: function(planId) {
        let removed = false;

        if (this._amortizationPlans[planId]) {
            delete this._amortizationPlans[planId];
            removed = true;
        }

        return removed;
    },

    _getFormRow: function(configuration) {
        let $formRow = $('<div class="fields">' +
            '<div class="three wide field">' +
                '<label>Configuración</label>' +
                `<input name="configuration" type="text" value="${configuration.name}" readonly="">` +
            '</div>' +
            '<div class="three wide field">' +
                '<label>Compra</label>' +
                `<input name="buy" type="text" value="${configuration.buy}" readonly="">` +
            '</div>' +
            '<div class="two wide field">' +
                '<label>Venta</label>' +
                `<input name="sell" type="text" value="${configuration.sell}" readonly="">` +
            '</div>' +
            '<div class="two wide field">' +
                '<label>Ahorro</label>' +
                `<input name="yearSaving" type="text" value="${configuration.yearSaving}" readonly="">` +
            '</div>' +
            '<div class="one wide field">' +
                '<label>IVA</label>' +
                `<input name="tax" type="text" value="${configuration.tax}" readonly="">` +
            '</div>' +
            `<div class="two wide field ${configuration.price && configuration.price > 0 ? '' : 'error'}">` +
                '<label>Coste instalación (€)</label>' +
                `<input name="price" type="text" class="onlynumbers editable-number" placeholder="Coste instalación" maxlength="5" data-configuration-id="${configuration.id}">` +
            '</div>' +
            '<div class="one wide field">' +
                '<label>Dto IBI (€)</label>' +
                `<input name="ibidto" type="text" class="onlynumbers editable-number" placeholder="Dto IBI" value="0" maxlength="4" data-configuration-id="${configuration.id}">` +
            '</div>' +
            '<div class="one wide field">' +
                '<label>Años dto</label>' +
                '<div class="ui mini selection dropdown">' +
                    `<input type="hidden" name="ibiyears" class="editable-number" data-configuration-id="${configuration.id}">` +
                    '<i class="dropdown icon"></i>' +
                    '<div class="text"></div>' +
                '</div>' +
            '</div>' +
            '<div class="one wide field">' +
                '<label>&nbsp;</label>' +
                `<button class="mini ui icon red button remove-configuration" data-configuration-id="${configuration.id}">` +
                    '<i class="trash icon"></i>' +
                '</button>' +
            '</div>' +
        '</div>');

        $formRow.find('.ui.dropdown').dropdown({
            values: this._getIbiDtoYears(0)
        });

        return $formRow;
    },
    _addRowToForm: function(configuration) {
        this._components['form'].find('.nocontent').remove();

        this._components['form'].append(this._getFormRow(configuration));
    },

    _initForm: function () {
        let self = this;
        this._components['form'].empty();

        if (this._configurations.length === 0) {
            this._components['form'].append($('<div class="ui segment nocontent"><p>No se ha seleccionado ninguna configuración</p></div>'));
        } else {
            this._configurations.forEach(configuration => self._addRowToForm(configuration));
        }
    },
    _refreshFigure: function () {
        let self = this;

        if (Object.keys(this._amortizationPlans).length === 0) {
            this._components['figure'].empty();
        } else {
            let data = Object.values(this._amortizationPlans).map(plan => self._getPlotlyFigureData(plan));
            Plotly.newPlot(this._config['figureId'], data, this._plotlyFigureLayout, this._plotlyFigureConfig)
        }
    },

    init: function () {
        let self = this;
        let elementId = this.element.attr('id');

        let titleId = `${elementId}-title`
        let $titleElement = $(`#${titleId}`);

        if ($titleElement.length === 0) {
            this.element.append($('<h2>', {id: titleId, text: 'Amortización'}))
        }

        if (!this._components['container']) {
            let containerId = `${elementId}-container`
            let $containerElement = $('<div>', {id: containerId});

            this._components['container'] = $containerElement;
            this.element.append($containerElement);
        }

        if (!this._components['config']) {
            let configId = `${elementId}-config`
            let $configElement = $(`<div id="${configId}" class="ui form">`  +
                '<div class="fields">' +
                    '<div class="fourteen wide field">' +
                        '<label>&nbsp;</label>' +
                    '</div>' +
                    '<div class="three wide field">' +
                        '<label>Vida instalación (Años)</label>' +
                        '<div id="installationLifeYears" class="ui selection dropdown">' +
                            '<input type="hidden" name="installationLifeYears">' +
                            '<i class="dropdown icon"></i>' +
                            '<div class="text"></div>' +
                        '</div>' +
                    '</div>' +
                    '<div class="three wide field">' +
                        '<label>Inflación anual energía (%)</label>' +
                        '<div id="energyPriceYearInflation" class="ui mini selection dropdown">' +
                            '<input type="hidden" name="energyPriceYearInflation">' +
                            '<i class="dropdown icon"></i>' +
                            '<div class="text"></div>' +
                        '</div>' +
                    '</div>' +
                '</div>' +
            '</div>');

            this._components['config'] = $configElement;
            this._components['container'].append($configElement);

            $configElement.find('#installationLifeYears').dropdown({
                values: this._getInstallationLifeYearsValues(this._config.installationLifeYears)
            });

            $configElement.find('#energyPriceYearInflation').dropdown({
                values: this._getEnergyPriceYearInflationValues(this._config.energyPriceYearInflation)
            });
        }

        if (!this._components['form']) {
            let formId = `${elementId}-form`
            let $formElement = $('<div>', {id: formId, 'class': 'ui mini form segment'});

            this._components['form'] = $formElement;
            this._components['container'].append($formElement);
        }

        if (!this._components['figure']) {
            let figureId = `${elementId}-figure`
            let $figureElement = $('<div>', {id: figureId});

            this._config['figureId'] = figureId;
            this._components['figure'] = $figureElement;
            this._components['container'].append($figureElement);
        }

        this._initForm();
        this._refreshFigure();

        this._bindEvents();
    },
    destroy: function () {
        this._destroy();
    },

    addConfiguration: function (inputConfiguration) {
        let configurationId = this._getConfigurationId(inputConfiguration);

        if (!this._configurationExists(configurationId)) {
            let configuration = {
                id: configurationId,
                name: inputConfiguration.name,
                buy: inputConfiguration.buy,
                sell: inputConfiguration.sell,
                tax: this._getTaxLabel(inputConfiguration),
                yearSaving: parseFloat(inputConfiguration.year_savings_eur),
                ibidto: 0,
                ibiyears: 0
            };

            this._configurations.push(configuration);

            this._addRowToForm(configuration);
            this._refreshFigure();
        }
    }
});
})();