(function () {
$.widget('ed.costestimation', {
    options: {
    },

    _tableConfig: {
        columns: [{
            data: 'name'
        }, {
            data: 'buy'
        }, {
            data: 'sell'
        }, {
            data: 'year_consumption_eur'
        }, {
            data: 'year_final_eur'
        }, {
            data: 'year_savings_eur'
        }, {
            data: 'configurationId',
            render: function(data, type, row) {
                return `<button class="mini ui icon blue button show-amortization" title="Ver amortización" data-configuration-id="${data}"><i class="signal icon"></i></button>`;
            }
        }],
        columnDefs: [{
            targets: 0,
            className: 'dt-body-left'
        }, {
            targets: 1,
            className: 'dt-body-left'
        }, {
            targets: 2,
            className: 'dt-body-left'
        }, {
            targets: 3,
            className: 'dt-body-right'
        }, {
            targets: 4,
            className: 'dt-body-right'
        }, {
            targets: 5,
            className: 'dt-body-right'
        }, {
            targets: 6,
            className: 'dt-body-right'
        }]
    },

    _components: {},
    _configurations: [],
    _taxConfig: {
        enabled: false,
        value: 21
    },

    _create: function () {
        this.DataFrame = dfjs.DataFrame;

        this._bindEvents();
        this._loadTaxConfiguration();

        console.log(`created widget costEstimation on ${this.element.attr('id')}`);
    },
    _setOption: function (key, value) {
        this.options[key] = value;
    },

    _bindEvents: function () {
        let self = this;

        this.element.on('click', '.show-amortization', handler => {
            let $element = $(handler.currentTarget);

            let rowIndex = self._components['table'].cell($element.parents('td')).index().row

            let data = self._components['table'].data()[rowIndex];

            this._trigger('showAmortization', null, data);
        });

        this.element.on('change', 'input[name="tax"]', handler => {
            let taxValue = $(handler.currentTarget).val();

            if (self._taxConfig.value !== taxValue) {
                self._taxConfig.value = taxValue

                self._persistTaxConfiguration();

                if (self._taxConfig.enabled) {
                    self._refresh();
                }
            }
        });
    },

    _configurationExists: function(configurationId) {
        return this._configurations.some(function(configuration) {
            return configuration.id === configurationId;
        });
    },
    _storeConfiguration: function(configuration) {
        this._configurations.push(configuration);
    },

    _isNumeric: function(str) {
        if (typeof str != "string") {
            return false
        }

        return !isNaN(str) && !isNaN(parseFloat(str))
    },
    _applyTax: function(value) {
        if (this._taxConfig.enabled) {
            return value * (1 + this._taxConfig.value / 100);
        }

        return value;
    },

    _refresh: function() {
        let self = this;
        let elementId = this.element.attr('id');

        let titleId = `${elementId}-title`
        let $titleElement = $(`#${titleId}`);

        if ($titleElement.length === 0) {
            this.element.append($('<h2>', {id: titleId, text: 'Coste/Ahorro estimado'}))
        }

        let taxFormId = `${elementId}-tax-form`
        let $taxFormElement = $(`#${taxFormId}`);

        if ($titleElement.length === 0) {
            let $newTaxFormElement = $(`<div id="${taxFormId}" class="ui form">`  +
                '<div class="fields">' +
                    '<div class="fourteen wide field">' +
                        '<label>&nbsp;</label>' +
                    '</div>' +
                    '<div class="one wide field">' +
                        '<label>IVA (%)</label>' +
                        `<input name="tax" type="text" value="${this._taxConfig.value}" maxlength="2">` +
                    '</div>' +
                    '<div class="one wide field">' +
                        '<label>Con IVA</label>' +
                        '<div class="ui slider checkbox checkbox-comp">' +
                            '<input type="radio" name="include_tax">' +
                            '<label>&nbsp;</label>' +
                        '</div>' +
                    '</div>' +
                '</div>' +
            '</div>');

            if (this._taxConfig.enabled) {
                $newTaxFormElement.find('input[name="include_tax"]').attr('checked', 'checked');
            }

            $newTaxFormElement.find('.checkbox-comp').checkbox({
                uncheckable: true,
                beforeChecked: function() {
                    let $taxFieldElement = $(this).parents('.fields').find('input[name="tax"]')
                    let tax = $taxFieldElement.val();

                    if (!self._isNumeric(tax) || parseInt(tax) === 0) {
                        $taxFieldElement.effect('shake', { times: 4, distance: 8 }, 750);
                        return false;
                    }

                    return true;
                },
                onChecked: function() {
                    self._taxConfig.enabled = true;
                    self._taxConfig.value = $(this).parents('.fields').find('input[name="tax"]').val();

                    self._persistTaxConfiguration();
                    self._refresh();
                },
                onUnchecked: function() {
                    self._taxConfig.enabled = false;

                    self._persistTaxConfiguration();
                    self._refresh();
                }
            });

            this.element.append($newTaxFormElement);
        }

        let tableId = `${elementId}-table`
        let $tableElement = $(`#${tableId}`);

        if ($tableElement.length === 0) {
            let $newTableElement = $('<table>', {id: tableId, 'class': 'dt ui celled table', 'dt-page-sizes': '-1'});

            $newTableElement
                .append(
                    $('<thead>').append(
                        $('<tr>').append(
                            $('<th>', {text: 'Configuración'}),
                            $('<th>', {text: 'Compra'}),
                            $('<th>', {text: 'Venta'}),
                            $('<th>', {text: 'Sin Autoconsumo'}),
                            $('<th>', {text: 'Con Autoconsumo'}),
                            $('<th>', {text: 'Ahorro'}),
                            $('<th>', {text: 'Acciones'})
                        )
                    )
                )
                .append(
                    $('<tbody>')
                );

            this.element.append($newTableElement);
        }

        if (this._components['table']) {
            this._components['table'].destroy();
        }

        let data = [];

        this._configurations.forEach(configuration => {
            data.push({
                id: configuration.id,
                name: configuration.name,
                buy: configuration.buy,
                sell: configuration.sell,
                year_consumption_eur: self._applyTax(configuration.data.year_consumption_eur).toFixed(2),
                year_final_eur: self._applyTax(configuration.data.year_final_eur).toFixed(2),
                year_savings_eur: self._applyTax(configuration.data.year_savings_eur).toFixed(2),
                with_tax: self._taxConfig.enabled,
                tax: self._taxConfig.value
            });
        });

        this._components['table'] = $(`#${tableId}`).DataTable({
            autoWidth: false,
            columnDefs: this._tableConfig.columnDefs,
            columns: this._tableConfig.columns,
            lengthMenu: [[-1], ['Todos']],
            data: data
        });
    },
    _persistTaxConfiguration: function() {
        sessionStorage.setItem('cost_estimation_tax_config', JSON.stringify(this._taxConfig));
    },
    _loadTaxConfiguration: function() {
        let taxConfig = sessionStorage.getItem('cost_estimation_tax_config');

        if (taxConfig) {
            this._taxConfig = JSON.parse(taxConfig);
        }
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
            if (!self._configurationExists(rc.id)) {
                let configuration = {
                    id: rc.id,
                    name: rc.name,
                    buy: 'PVPC',
                    sell: 'PVPC',
                    data: {}
                };

                let df = new self.DataFrame(rc.dataframe_m.data, rc.dataframe_m.columns);

                let aggregations = df
                    .withColumn('ag', (row) => 1)
                    .groupBy('ag')
                    .aggregate((group) => [
                        group.stat.sum('month_consumption_eur'),
                        group.stat.sum('month_simplified_final_eur'),
                        group.stat.sum('month_pvpc_simplified_savings_eur')
                    ])
                    .toDict()['aggregation'][0];

                configuration.data['year_consumption_eur'] = Math.round(aggregations[0] * 100) / 100;
                configuration.data['year_final_eur'] = Math.round(aggregations[1] * 100) / 100;
                configuration.data['year_savings_eur'] = Math.round(aggregations[2] * 100) / 100;

                self._storeConfiguration(configuration);

                const colPrefix = 'month_period_';
                const colBuySuffixes = ['_kwh', '_finalconsumption_kwh']

                let offerPairs = offerdb.getBuySellOfferPairs();

                offerPairs.forEach(function(offerPair) {
                    Object.keys(offerPair.pairs).forEach(function(pairRateType) {
                        let pair = offerPair.pairs[pairRateType];

                        colBuySuffixes.forEach(function(colSuffix) {
                            let groupToSum = [];

                            pair.buyOffer.periods.forEach(function(period) {
                                let periodName = period[0];
                                let periodBuyPrice = period[1];

                                let colName = `${colPrefix}${pairRateType}_${periodName}${colSuffix}`;

                                let newColumnName = colName.replaceAll('_kwh', '_eur');

                                df = df.withColumn(newColumnName, (row) => row.get(colName) * periodBuyPrice);

                                groupToSum.push(newColumnName);
                            });

                            let newColumnName = `month_periods_${pairRateType}${colSuffix}`.replaceAll('_kwh', '_eur');

                            df = df.withColumn(newColumnName, (row) => groupToSum.reduce((acc, colName) => acc + row.get(colName), 0));
                        });

                        let sellPrice = pair.sellOffer.price;

                        let exceedingColumnName = `month_periods_${pairRateType}_exceeding_eur`;
                        df = df.withColumn(exceedingColumnName, (row) => row.get('month_exceeding_kwh') * sellPrice);

                        let consumptionColumnName = `month_periods_${pairRateType}_eur`;
                        let exceedingFixedColumnName = `month_periods_${pairRateType}_exceeding_fixed_eur`;
                        let finalConsumptionColumnName = `month_periods_${pairRateType}_finalconsumption_eur`;
                        let finalColumnName = `month_periods_${pairRateType}_final_eur`;
                        let savingsColumnName = `month_periods_${pairRateType}_savings_eur`;

                        df = df.withColumn(exceedingFixedColumnName, (row) => (row.get(exceedingColumnName) > row.get(finalConsumptionColumnName)) ? row.get(finalConsumptionColumnName) : row.get(exceedingColumnName));
                        df = df.withColumn(finalColumnName, (row) => row.get(finalConsumptionColumnName) - row.get(exceedingFixedColumnName));
                        df = df.withColumn(savingsColumnName, (row) => row.get(consumptionColumnName) - row.get(finalColumnName));

                        let aggregations = df
                            .withColumn('ag', (row) => 1)
                            .groupBy('ag')
                            .aggregate((group) => [
                                group.stat.sum(consumptionColumnName),
                                group.stat.sum(finalColumnName),
                                group.stat.sum(savingsColumnName),
                            ])
                            .toDict()['aggregation'][0];

                        self._storeConfiguration({
                            id: rc.id,
                            name: rc.name,
                            buy: `${offerPair.company.name} - ${pairRateType}`,
                            sell: offerPair.company.name,
                            data: {
                                year_consumption_eur: Math.round(aggregations[0] * 100) / 100,
                                year_final_eur: Math.round(aggregations[1] * 100) / 100,
                                year_savings_eur: Math.round(aggregations[2] * 100) / 100
                            }
                        });

                        df = df.withColumn(exceedingColumnName, (row) => row.get('month_exceeding_simplified_eur'));
                        df = df.withColumn(exceedingFixedColumnName, (row) => (row.get(exceedingColumnName) > row.get(finalConsumptionColumnName)) ? row.get(finalConsumptionColumnName) : row.get(exceedingColumnName));
                        df = df.withColumn(finalColumnName, (row) => row.get(finalConsumptionColumnName) - row.get(exceedingFixedColumnName));
                        df = df.withColumn(savingsColumnName, (row) => row.get(consumptionColumnName) - row.get(finalColumnName));

                        aggregations = df
                            .withColumn('ag', (row) => 1)
                            .groupBy('ag')
                            .aggregate((group) => [
                                group.stat.sum(consumptionColumnName),
                                group.stat.sum(finalColumnName),
                                group.stat.sum(savingsColumnName),
                            ])
                            .toDict()['aggregation'][0];

                        self._storeConfiguration({
                            id: rc.id,
                            name: rc.name,
                            buy: `${offerPair.company.name} - ${pairRateType}`,
                            sell: 'PVPC',
                            data: {
                                year_consumption_eur: Math.round(aggregations[0] * 100) / 100,
                                year_final_eur: Math.round(aggregations[1] * 100) / 100,
                                year_savings_eur: Math.round(aggregations[2] * 100) / 100
                            }
                        });
                    });
                });
            }
        });

        this._refresh();
    },
    removeConfiguration: function(configurationId) {
        let remainingConfigurations = this._configurations.filter(function(configuration) {
            return configuration.id !== configurationId;
        });

        if (remainingConfigurations.length !== this._configurations.length) {
            this._configurations = remainingConfigurations;
            this._refresh();
        }

        if (this._configurations.length == 0) {
            this.element.empty();
        }
    }
});
})();