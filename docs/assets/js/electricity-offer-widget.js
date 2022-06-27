(function () {
$.widget('ed.electricityoffer', {
    options: {
        calendarConfig: {
            type: 'date',
            firstDayOfWeek: 1,
            maxDate: new Date(),
            text: {
                days: ['D', 'L', 'M', 'X', 'J', 'V', 'S'],
                months: ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'],
                monthsShort: ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'],
                today: 'Hoy',
                now: 'Ahora',
                am: 'AM',
                pm: 'PM'
            },
            formatter: {
                date: function (date, settings) {
                    if (!date) return '';
                    var day = date.getDate();
                    var month = date.getMonth() + 1;
                    var year = date.getFullYear();

                    return day.toString().padStart(2, '0') + '/' + month.toString().padStart(2, '0') + '/' + year;
                }
            }
        }
    },

    _elements: {},

    _offers: [{
        company: {
            id: 'totalenergies',
            name: 'TotalEnergies',
        },
        buyOffers: [{
            id: 1,
            rateType: '20td',
            company: {
                id: 'totalenergies',
                name: 'TotalEnergies',
            },
            date: '01/01/2022',
            periods: [['P1', 0.17473], ['P2', 0.12433], ['P3', 0.1047]],
            rawPeriods: [['P1', '0.28297'], ['P2', '0.20134'], ['P3', '0.16955']],
            dto: '5>35'
        }, {
            id: 2,
            rateType: 'wk',
            company: {
                id: 'totalenergies',
                name: 'TotalEnergies',
            },
            date: '01/01/2022',
            periods: [['P1', 0.14892], ['P3', 0.10534]],
            rawPeriods: [['P1', '0.24117'], ['P3', '0.17059']],
            dto: '5>35'
        }, {
            id: 3,
            rateType: 'fix',
            company: {
                id: 'totalenergies',
                name: 'TotalEnergies',
            },
            date: '0101/2022',
            periods: [['P1', 0.13052]],
            rawPeriods: [['P1', '0.21137']],
            dto: '5>35'
        }],
        sellOffers: [{
            company: {
                id: 'totalenergies',
                name: 'TotalEnergies',
            },
            date: '01/01/2022',
            price: 0.07
        }]
    }, {
        company: {
            id: 'holaluz',
            name: 'HolaLuz',
        },
        buyOffers: [{
            id: 4,
            rateType: '20td',
            company: {
                id: 'holaluz',
                name: 'HolaLuz',
            },
            date: '01/01/2022',
            periods: [['P1', 0.413], ['P2', 0.326], ['P3', 0.258]],
            rawPeriods: [['P1', '0.413'], ['P2', '0.326'], ['P3', '0.258']],
            dto: 0
        }],
        sellOffers: []
    }],

    _hash(str, seed = 0) {
        let h1 = 0xdeadbeef ^ seed, h2 = 0x41c6ce57 ^ seed;
        for (let i = 0, ch; i < str.length; i++) {
            ch = str.charCodeAt(i);
            h1 = Math.imul(h1 ^ ch, 2654435761);
            h2 = Math.imul(h2 ^ ch, 1597334677);
        }
        h1 = Math.imul(h1 ^ (h1>>>16), 2246822507) ^ Math.imul(h2 ^ (h2>>>13), 3266489909);
        h2 = Math.imul(h2 ^ (h2>>>16), 2246822507) ^ Math.imul(h1 ^ (h1>>>13), 3266489909);
        return (4294967296 * (2097151 & h2) + (h1>>>0)).toString().padStart(16, '0');
    },

    _create: function () {
        let $companyAddButton = $('<i>', {id: 'company-add', 'class': 'link plus circle blue icon'})

        this.element.append($companyAddButton);
        this._elements['companyAddButton'] = $companyAddButton;

        this.element.append(
            $('<h2>', {'class': 'ui header'})
                .append($companyAddButton)
                .append($('<div>', {'class': 'content', 'text': 'Ofertas de compra/venta de electricidad'}))
        );

        let $mainOffersContainer = $('<div id="main-offers-container" style="margin-top: 2em;">')

        this.element.append($mainOffersContainer);
        this._elements['mainOffersContainer'] = $mainOffersContainer;

        this.element.append($('<div class="ui divider"></div>'));

        this.element.append($('<form id="configuration-form" class="ui form">' +
            '<input type="file" id="import-file" accept="*.json" style="display:none">' +
            '<div class="ui right floated floating labeled icon dropdown yellow button">' +
                '<i class="upload icon"></i>' +
                '<span class="text">Importar</span>' +
                '<div class="menu">' +
                    '<div id="import_url_modal_button" class="item" data-modal-id="import_url_modal"><i class="linkify icon"></i> URL</div>' +
                    '<div id="import_file_button" class="item"><i class="file icon"></i> Fichero</div>' +
                '</div>' +
            '</div>' +
            '<button id="export-button" class="ui right floated labeled icon green button"  type="button" data-filename="configuration.json">' +
                '<i class="download icon"></i>' +
                'Exportar' +
            '</button>' +
        '</form>'))

        this.element.append($('<div id="import_url_modal" class="ui modal" data-trigger-button="import_url_modal_button">' +
            '<h2 class="ui header">Importar configuración remota</h2>' +
            '<div class="content">' +
                '<form class="ui form">' +
                    '<div class="field">' +
                        '<label>URL</label>' +
                        '<input type="text" name="import_url" value="https://raw.githubusercontent.com/' + this.options.githubUsername + '/energydatum/main/' + this.options.configurationBackupFilename + '">' +
                    '</div>' +
                '</form>' +
            '</div>' +
            '<div class="actions">' +
                '<div id="import_url_button" class="ui yellow ok button">' +
                    '<i class="check icon"></i>' +
                    'Importar' +
                '</div>' +
            '</div>' +
        '</div>'))

        this._loadData();
        this._refresh();
        this._bindEvents();

        console.log(`created widget electricityoffer on ${this.element.attr('id')}`);
    },
    _setOption: function (key, value) {
        this.options[key] = value;
    },

    _loadData: function () {
        let buyOffersIds = Object.keys(localStorage).filter(key => key.match(/buy_offer_\d+/));

        let buyOffers = buyOffersIds.map(id => JSON.parse(localStorage.getItem(id)));

        let offers = buyOffers.reduce((acc, offer) => {
            let companyIndex = acc.findIndex(c => c.company.id === offer.company.id);

            if (companyIndex === -1) {
                acc.push({
                    company: offer.company,
                    buyOffers: [offer],
                    sellOffers: []
                });
            } else {
                acc[companyIndex].buyOffers.push(offer);
            }

            return acc;
        }, []);

        let sellOffersIds = Object.keys(localStorage).filter(key => key.match(/sell_offer_\d+/));

        let sellOffers = sellOffersIds.map(id => JSON.parse(localStorage.getItem(id)));

        offers = sellOffers.reduce((acc, offer) => {
            let companyIndex = acc.findIndex(c => c.company.id === offer.company.id);

            if (companyIndex === -1) {
                acc.push({
                    company: offer.company,
                    buyOffers: [],
                    sellOffers: [offer]
                });
            } else {
                acc[companyIndex].sellOffers.push(offer);
            }

            return acc;
        }, offers);

        this._offers = offers;
    },

    _refresh: function () {
        let self = this;

        self._elements['mainOffersContainer'].empty();

        this._offers.forEach(offer => {
            self._elements['mainOffersContainer'].append(self._createCompanyReadView(offer));
        });

        this._checkCompanyNoContent();
    },

    _bindEvents: function () {
        let self = this;

        this.element.find('#configuration-form .ui.dropdown').dropdown({
            on: 'hover',
            action: 'hide'
        });

        this.element.find('.ui.modal').each((i, elem) => {
            let triggerButton = $(elem).data('trigger-button');

            self.element.find(`#${triggerButton}`).click(handler => {
                let modalId = $(handler.currentTarget).data('modal-id');

                self.element.find(`#${modalId}`).modal('show');
            });
        });

        this.element.find('button#export-button').click(handler => {
            handler.preventDefault();
            let $element = $(handler.currentTarget);
            let fileName = $element.data('filename');

            if ('Blob' in window) {
                var blob = new Blob([JSON.stringify(self._offers)], {type: 'application/json'});

                if ('msSaveOrOpenBlob' in navigator) {
                    navigator.msSaveOrOpenBlob(blob, fileName);
                } else {
                    var downloadLinkElement = document.createElement('a');
                    downloadLinkElement.download = fileName;
                    downloadLinkElement.innerHTML = 'Download File';

                    if ('webkitURL' in window) {
                        downloadLinkElement.href = window.webkitURL.createObjectURL(blob);
                    } else {
                        downloadLinkElement.href = window.URL.createObjectURL(blob);
                    }

                    downloadLinkElement.onclick = event => {
                        document.body.removeChild(event.target);
                    };

                    downloadLinkElement.style.display = 'none';
                    document.body.appendChild(downloadLinkElement);

                    downloadLinkElement.click();
                }
            } else {
                alert('Your browser does not support the HTML5 Blob.');
            }
        });

        this.element.find('#import_file_button').click(handler => {
            handler.preventDefault();

            if ('FileReader' in window) {
                self.element.find('input#import-file').click();
            } else {
                alert('Your browser does not support the HTML5 FileReader.');
            }
        });

        this.element.find('input#import-file').change(event => {
            var fileToLoad = event.target.files[0];

            if (fileToLoad) {
                var fileReader = new FileReader();

                fileReader.onload = function(fileLoadedEvent) {
                    var textFromFileLoaded = fileLoadedEvent.target.result;

                    self._loadFromFile(JSON.parse(textFromFileLoaded));

                    self._refresh();
                };

                fileReader.readAsText(fileToLoad, 'UTF-8');
            }
        });

        this.element.find('#import_url_button').click(handler => {
            handler.preventDefault();
            let $form = $(handler.currentTarget).parents('.ui.modal').find('.ui.form');
            let url = $form.find('input[name="import_url"]').val().trim();

            $.get(url).done(data => {
                self._loadFromFile(JSON.parse(data));

                self._refresh();
            });
        });

        this._elements['companyAddButton'].click(event => {
            let $companyInsertViewElement = self._createCompanyInsertView();

            self._elements['mainOffersContainer'].append($companyInsertViewElement);

            self._checkCompanyNoContent();

            $('html, body').animate({
                scrollTop: $companyInsertViewElement.offset().top
            }, 750).promise().done(() => {
                $companyInsertViewElement.effect('shake', { times: 4, distance: 8 }, 750)
            });
        });

        this.element.on('click', '.company-remove', event => {
            let $companyContainer = $(event.currentTarget).parents('.company-container');
            let companyId = $companyContainer.data('company-id');

            self._unpersistCompany(companyId);

            $companyContainer.remove();

            self._checkCompanyNoContent();
        });

        this.element.on('click', '.company-cancel', event => {
            let $element = $(event.currentTarget);

            $element.parents('.company-form').remove();

            self._checkCompanyNoContent();
        });

        this.element.on('click', '.company-save', event => {
            let $companyForm = $(event.currentTarget).parents('.company-form');
            let $companyField = $companyForm.find('input[name="company"]')

            let companyName = $companyField.val();

            if (!companyName) {
                $companyField.parents('div.field').effect('shake', { times: 4, distance: 8 }, 750);
            } else {
                if (self._isNewCompany(companyName)) {
                    let company = self._persistCompany(companyName);

                    $companyForm.remove();

                    self._elements['mainOffersContainer'].append(self._createCompanyReadView(company));
                } else {
                    let companyId = self._getCompanyId(companyName);
                    self._elements['mainOffersContainer'].find(`.company-container[data-company-id="${companyId}"]`).effect('shake', { times: 4, distance: 8 }, 750);
                }
            }
        });

        this.element.on('click', '.buy-offer-add', event => {
            let $element = $(event.currentTarget);
            let rateType = $element.data('rate-type');
            let companyId = $element.data('company-id');

            let buyOffersContainerId = `company-buy-offers-${rateType}-container`

            let $offersContainer = $(`div#${buyOffersContainerId}[data-company-id="${companyId}"]`);

            let $buyOfferView = self._getBuyOfferInsertView(rateType, companyId);

            $offersContainer.find(`.buy-offer-${rateType}-nocontent`).remove();
            $offersContainer.append($buyOfferView);
        });

        this.element.on('click', '.buy-offer-cancel', event => {
            let $element = $(event.currentTarget);
            let rateType = $element.data('rate-type');

            let buyOffersContainerId = `company-buy-offers-${rateType}-container`

            let $offersContainer = $element.parents(`#${buyOffersContainerId}`);

            $element.parents('.fields').remove();

            if ($offersContainer.find('.fields').length === 0) {
                $offersContainer.append($('<div>', {'class': `ui message buy-offer-${rateType}-nocontent`, 'text': 'No hay ofertas'}));
            }
        });

        this.element.on('click', '.buy-offer-save', event => {
            let $element = $(event.currentTarget);
            let rateType = $element.data('rate-type');

            let buyOffersContainerId = `company-buy-offers-${rateType}-container`

            let $offersContainer = $element.parents(`#${buyOffersContainerId}`);

            let companyId = $offersContainer.data('company-id');
            let companyName = $offersContainer.data('company-name');

            let $fields = $element.closest('.fields');

            let date = $fields.find('input[name="date"]').val();
            let periods = [];

            $fields.find('input.period').each((i, elem) => {
                $elem = $(elem)
                periods.push([$elem.attr('name'), $elem.val()]);
            });

            let dto = $fields.find('input[name="dto"]').val() || 0;

            let offer = self._persistBuyOffer(rateType, companyId, companyName, date, periods, dto);

            $fields.remove();

            $offersContainer.append(self._getBuyOfferReadView(offer));
        });

        this.element.on('click', '.buy-offer-remove', event => {
            let $element = $(event.currentTarget);
            let rateType = $element.data('rate-type');
            let companyId = $element.data('company-id');
            let offerId = $element.data('offer-id');

            let buyOffersContainerId = `company-buy-offers-${rateType}-container`

            let $offersContainer = $element.parents(`#${buyOffersContainerId}`);

            self._unpersistBuyOffer(companyId, offerId);

            $element.parents('.fields').remove();

            if ($offersContainer.find('.fields').length === 0) {
                $offersContainer.append($('<div>', {'class': `ui message buy-offer-${rateType}-nocontent`, 'text': 'No hay ofertas'}));
            }
        });

        this.element.on('click', '.sell-offer-add', event => {
            let $element = $(event.currentTarget);
            let companyId = $element.data('company-id');

            let $offersContainer = $(`div#company-sell-offers-container[data-company-id="${companyId}"]`);

            let $offerView = self._getSellOfferInsertView(companyId);

            $offersContainer.find(`.sell-offer-nocontent`).remove();
            $offersContainer.append($offerView);
        });

        this.element.on('click', '.sell-offer-cancel', event => {
            let $element = $(event.currentTarget);

            let $offersContainer = $element.parents('#company-sell-offers-container');

            $element.parents('.fields').remove();

            if ($offersContainer.find('.fields').length === 0) {
                $offersContainer.append($('<div>', {'class': 'ui message buy-offer-nocontent', 'text': 'No hay ofertas'}));
            }
        });

        this.element.on('click', '.sell-offer-save', event => {
            let $element = $(event.currentTarget);

            let $offersContainer = $element.parents('#company-sell-offers-container');

            let companyId = $offersContainer.data('company-id');
            let companyName = $offersContainer.data('company-name');

            let $fields = $element.closest('.fields');

            let date = $fields.find('input[name="date"]').val();
            let price = $fields.find('input[name="price"]').val() || 0;

            let offer = self._persistSellOffer(companyId, companyName, date, price);

            $fields.remove();

            $offersContainer.append(self._getSellOfferReadView(offer));
        });

        this.element.on('click', '.sell-offer-remove', event => {
            let $element = $(event.currentTarget);
            let companyId = $element.data('company-id');
            let offerId = $element.data('offer-id');

            let $offersContainer = $element.parents('#company-sell-offers-container');

            self._unpersistSellOffer(companyId, offerId);

            $element.parents('.fields').remove();

            if ($offersContainer.find('.fields').length === 0) {
                $offersContainer.append($('<div>', {'class': 'ui message sell-offer-nocontent', 'text': 'No hay ofertas'}));
            }
        });
    },

    _checkCompanyNoContent: function () {
        if (this._elements['mainOffersContainer'].find('.company-container').length == 0 && this._elements['mainOffersContainer'].find('.company-form').length == 0) {
            this._elements['mainOffersContainer'].append($('<div>', {'class': 'ui message company-nocontent', 'text': 'No hay ofertas'}));
        } else {
            this._elements['mainOffersContainer'].find('.company-nocontent').remove();
        }
    },
    _createCompanyReadView: function (offer) {
        let $companyBuyOffers = $('<div>', {'class': 'ui segment company-buy-offers'});

        $companyBuyOffers
            .append($('<h4>', {'class': 'ui header', 'text': 'Ofertas de compra'}))
            .append(this._getBuyOfferTitle('20td', offer.company.id))
            .append(
                this._getBuyOfferContainer('20td', offer.company)
                    .append(
                        this._getBuyOffersReadView('20td', offer)
                    )
            )
            .append(this._getBuyOfferTitle('wk', offer.company.id))
            .append(
                this._getBuyOfferContainer('wk', offer.company)
                    .append(
                        this._getBuyOffersReadView('wk', offer)
                    )
            )
            .append(this._getBuyOfferTitle('fix', offer.company.id))
            .append(
                this._getBuyOfferContainer('fix', offer.company)
                    .append(
                        this._getBuyOffersReadView('fix', offer)
                    )
            );

        let $companySellOffers = $('<div>', {'class': 'ui segment company-sell-offers'});

        $companySellOffers
            .append(this._getSellOfferTitle(offer.company.id))
            .append(
                this._getSellOfferContainer(offer.company)
                    .append(
                        this._getSellOffersReadView(offer)
                    )
            );

        return $('<div>', {'data-company-id': offer.company.id, 'class': 'ui segment company-container'})
            .append(
                $('<h3>', {'class': 'ui header'})
                    .append($('<i>', {'class': 'link trash red small icon company-remove'}))
                    .append($('<div>', {text: offer.company.name, 'class': 'content'}))
            )
            .append($companyBuyOffers)
            .append($companySellOffers);
    },
    _createCompanyInsertView: function () {
        let html = '<div class="ui segment form company-form">' +
            '<div class="fields">' +
                '<div class="three wide field">' +
                    '<label>Compañía</label>' +
                    '<input name="company" type="text" placeholder="Compañía" maxlength="25">' +
                '</div>' +
                '<div class="two wide field">' +
                    '<label>&nbsp;</label>' +
                    '<div class="ui icon green button company-save" data-rate-type="20td">' +
                        '<i class="save icon"></i>' +
                    '</div>' +
                    '<div class="ui icon button company-cancel" data-rate-type="20td">' +
                        '<i class="times icon"></i>' +
                    '</div>' +
                '</div>' +
            '</div>' +
        '</div>';

        return $(html);
    },

    _getBuyOfferTitle: function (rateType, companyId) {
        const rateTypeTitles = {
            '20td': 'Tarifa 2.0TD',
            'wk': 'Tarifa Fin de Semana',
            'fix': 'Tarifa Precio Fijo'
        };

        return $('<h5>', {'class': 'ui header'})
            .append($('<i>', {'class': 'link plus circle blue icon buy-offer-add', 'data-rate-type': rateType, 'data-company-id': companyId}))
            .append($('<div>', {text: rateTypeTitles[rateType], 'class': 'content'}))
    },
    _getBuyOfferContainer: function (rateType, company) {
        return $('<div>', {id: `company-buy-offers-${rateType}-container`, 'class': 'ui form', 'data-company-id': company.id, 'data-company-name': company.name});
    },

    _getSellOfferTitle: function (companyId) {
        return $('<h4>', {'class': 'ui header'})
            .append($('<i>', {'class': 'link plus circle blue icon sell-offer-add', 'data-company-id': companyId}))
            .append($('<div>', {text: 'Ofertas de venta', 'class': 'content'}));
    },
    _getSellOfferContainer: function (company) {
        return $('<div>', {'id': 'company-sell-offers-container', 'class': 'ui form', 'data-company-id': company.id, 'data-company-name': company.name});
    },

    _getBuyOffersReadView: function (rateType, offer) {
        let self = this;

        let $readViews = offer.buyOffers.filter(buyOffer => buyOffer.rateType === rateType).map(buyOffer => {
            return self._getBuyOfferReadView(buyOffer);
        });

        if ($readViews.length === 0) {
            $readViews = $('<div>', {'class': `ui message buy-offer-${rateType}-nocontent`, 'text': 'No hay ofertas'})
        }

        return $readViews;
    },
    _getBuyOfferReadView: function (buyOffer) {
        let self = this;

        const periodsTemplate = {
            '20td': '<div class="two wide field">' +
                '<label>P1</label>' +
                '<input name="P1" type="text" readonly="">' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>P2</label>' +
                '<input name="P2" type="text" readonly="">' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>P3</label>' +
                '<input name="P3" type="text" readonly="">' +
            '</div>',
            'wk': '<div class="two wide field">' +
                '<label>P1</label>' +
                '<input name="P1" type="text" readonly="">' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>&nbsp;</label>' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>P3</label>' +
                '<input name="P3" type="text" readonly="">' +
            '</div>',
            'fix': '<div class="two wide field">' +
                '<label>P1</label>' +
                '<input name="P1" type="text" readonly="">' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>&nbsp;</label>' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>&nbsp;</label>' +
            '</div>'
        }

        const htmlPreffix = '<div class="fields">' +
            '<div class="one wide field">' +
                '<label>En uso</label>' +
                '<div class="ui slider checkbox checkbox-comp">' +
                    '<input type="radio" name="current_buy">' +
                    '<label>&nbsp;</label>' +
                '</div>' +
            '</div>' +
            '<div class="three wide field">' +
                '<label>Fecha oferta</label>' +
                '<input name="date" type="text" readonly="">' +
            '</div>';

        const htmlSuffix =
            '<div class="two wide field">' +
                '<label>&nbsp;</label>' +
            '</div>' +
            '<div class="one wide field">' +
                '<label>&nbsp;</label>' +
                '<div class="ui icon red button buy-offer-remove" data-rate-type="' + buyOffer.rateType + '" data-company-id="' + buyOffer.company.id + '" data-offer-id="' + buyOffer.id + '">' +
                    '<i class="trash icon"></i>' +
                '</div>' +
            '</div>' +
        '</div>';

        let html = htmlPreffix + periodsTemplate[buyOffer.rateType] + htmlSuffix;

        let $readView = $(html);

        $readView.attr('data-offer-id', buyOffer.id);
        $readView.attr('data-company-id', buyOffer.company.id);
        $readView.find('input[name="date"]').val(buyOffer.date);

        if (this._buyOfferIsCurrent(buyOffer.id)) {
            $readView.find('input[name="current_buy"]').attr('checked', 'checked');
        }

        buyOffer.periods.forEach((period, i) => {
            $periodInput = $readView.find(`input[name=${period[0]}]`);
            $periodInput.val(period[1]);

            if (buyOffer.dto) {
                $periodLabel = $periodInput.siblings('label');

                $periodLabel.append($('<i>', { 'class': 'info circle icon popup-comp', 'data-title': 'Precio original', 'data-content': `${buyOffer.rawPeriods[i][1]} € (Sin dto del ${buyOffer.dto} %)` }));
            }
        });

        $readView.find('.popup-comp').popup();

        $readView.find('.checkbox-comp').checkbox({
            uncheckable: true,
            onChecked: function() {
                let $element = $(this);

                self._setBuyOfferAsCurrent($element.parents('.fields').data('offer-id'));
            },
            onUnchecked: function() {
                let $element = $(this);

                self._unsetBuyOfferAsCurrent($element.parents('.fields').data('offer-id'));
            }
        });

        return $readView;
    },
    _getBuyOfferInsertView: function (rateType, companyId) {
        const periodsTemplate = {
            '20td': '<div class="two wide field">' +
                '<label>P1</label>' +
                '<input name="P1" type="text" class="period" placeholder="P1" maxlength="7">' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>P2</label>' +
                '<input name="P2" type="text" class="period" placeholder="P2" maxlength="7">' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>P3</label>' +
                '<input name="P3" type="text" class="period" placeholder="P3" maxlength="7">' +
            '</div>',
            'wk': '<div class="two wide field">' +
                '<label>P1</label>' +
                '<input name="P1" type="text" class="period" placeholder="P1" maxlength="7">' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>&nbsp;</label>' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>P3</label>' +
                '<input name="P3" type="text" class="period" placeholder="P3" maxlength="7">' +
            '</div>',
            'fix': '<div class="two wide field">' +
                '<label>P1</label>' +
                '<input name="P1" type="text" class="period" placeholder="P1" maxlength="7">' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>&nbsp;</label>' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>&nbsp;</label>' +
            '</div>'
        };

        const htmlPreffix = '<div class="fields">' +
            '<div class="one wide field">' +
                '<label>&nbsp;</label>' +
            '</div>' +
            '<div class="ui calendar three wide field calendar-comp">' +
                '<label>Fecha oferta</label>' +
                '<div class="ui input left icon">' +
                    '<i class="calendar icon"></i>' +
                    '<input name="date" type="text" placeholder="Fecha oferta" maxlength="10">' +
                '</div>' +
            '</div>';

        const htmlSuffix = '<div class="two wide field">' +
                '<label>Dto <i class="info circle icon popup-comp" data-title="Descuentos múltiples" data-content="dto1>dto2>...>dtoN"></i></label>' +
                '<input name="dto" type="text" placeholder="%" maxlength="10">' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>&nbsp;</label>' +
                '<div class="ui icon green button buy-offer-save" data-rate-type="' + rateType + '">' +
                    '<i class="save icon"></i>' +
                '</div>' +
                '<div class="ui icon button buy-offer-cancel" data-rate-type="' + rateType + '">' +
                    '<i class="times icon"></i>' +
                '</div>' +
            '</div>' +
        '</div>';

        let html = htmlPreffix + periodsTemplate[rateType] + htmlSuffix;

        let $insertView = $(html);

        $insertView.find('.calendar-comp').calendar(this.options.calendarConfig);

        $insertView.find('.popup-comp').popup();

        return $insertView;
    },
    _getSellOffersReadView: function (offer) {
        let self = this;

        let $readViews = offer.sellOffers.map(sellOffer => {
            return self._getSellOfferReadView(sellOffer);
        });

        if ($readViews.length === 0) {
            $readViews = $('<div>', {'class': `ui message sell-offer-nocontent`, 'text': 'No hay ofertas'})
        }

        return $readViews;
    },
    _getSellOfferReadView: function (sellOffer) {
        let self = this;

        const html = '<div class="fields">' +
            '<div class="one wide field">' +
                '<label>En uso</label>' +
                '<div class="ui slider checkbox checkbox-comp">' +
                    '<input type="radio" name="current_sell">' +
                    '<label>&nbsp;</label>' +
                '</div>' +
            '</div>' +
            '<div class="three wide field">' +
                '<label>Fecha oferta</label>' +
                '<input name="date" type="text" readonly="">' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>Precio</label>' +
                '<input name="price" type="text" placeholder="Precio" maxlength="7">' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>&nbsp;</label>' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>&nbsp;</label>' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>&nbsp;</label>' +
            '</div>' +
            '<div class="one wide field">' +
                '<label>&nbsp;</label>' +
                '<div class="ui icon red button sell-offer-remove" data-company-id="' + sellOffer.company.id + '" data-offer-id="' + sellOffer.id + '">' +
                    '<i class="trash icon"></i>' +
                '</div>' +
            '</div>' +
        '</div>';

        let $readView = $(html);

        $readView.attr('data-offer-id', sellOffer.id);
        $readView.attr('data-company-id', sellOffer.company.id);
        $readView.find('input[name="date"]').val(sellOffer.date);
        $readView.find('input[name="price"]').val(sellOffer.price);

        if (this._sellOfferIsCurrent(sellOffer.id)) {
            $readView.find('input[name="current_sell"]').attr('checked', 'checked');
        }

        $readView.find('.checkbox-comp').checkbox({
            uncheckable: true,
            onChecked: function() {
                let $element = $(this);

                self._setSellOfferAsCurrent($element.parents('.fields').data('offer-id'));
            },
            onUnchecked: function() {
                let $element = $(this);

                self._unsetSellOfferAsCurrent($element.parents('.fields').data('offer-id'));
            }
        });

        return $readView;
    },
    _getSellOfferInsertView: function (companyId) {
        const html = '<div class="fields">' +
            '<div class="one wide field">' +
                '<label>&nbsp;</label>' +
            '</div>' +
            '<div class="ui calendar three wide field calendar-comp">' +
                '<label>Fecha oferta</label>' +
                '<div class="ui input left icon">' +
                    '<i class="calendar icon"></i>' +
                    '<input name="date" type="text" placeholder="Fecha oferta" maxlength="10">' +
                '</div>' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>Precio</label>' +
                '<input name="price" type="text" placeholder="Precio" maxlength="7">' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>&nbsp;</label>' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>&nbsp;</label>' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>&nbsp;</label>' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>&nbsp;</label>' +
                '<div class="ui icon green button sell-offer-save">' +
                    '<i class="save icon"></i>' +
                '</div>' +
                '<div class="ui icon button sell-offer-cancel">' +
                    '<i class="times icon"></i>' +
                '</div>' +
            '</div>' +
        '</div>';

        let $insertView = $(html);

        $insertView.find('.calendar-comp').calendar(this.options.calendarConfig);

        return $insertView;
    },

    _getCompanyId: function (company) {
        return company.trim().toLowerCase()
            .replace(/ /g, '-')
            .replace(/á/g, 'a')
            .replace(/é/g, 'e')
            .replace(/í/g, 'i')
            .replace(/ó/g, 'ó')
            .replace(/ú/g, 'u')
            .replace(/ñ/g, 'n');
    },
    _isNewCompany: function (companyName) {
        let companyId = this._getCompanyId(companyName);

        let index = this._offers.findIndex(offer => offer.company.id === companyId);

        return index === -1;
    },
    _loadFromFile: function (companies) {
        Object.keys(localStorage)
            .filter(key => key.startsWith('buy_offer_'))
            .forEach(key => localStorage.removeItem(key));

        Object.keys(localStorage)
            .filter(key => key.startsWith('sell_offer_'))
            .forEach(key => localStorage.removeItem(key));

        companies.forEach(company => {
            company.buyOffers.forEach(buyOffer => {
                localStorage.setItem(buyOffer.id, JSON.stringify(buyOffer));
            });

            company.sellOffers.forEach(sellOffer => {
                localStorage.setItem(sellOffer.id, JSON.stringify(sellOffer));
            });
        });

        this._offers = companies;
    },
    _persistCompany: function (companyName) {
        let companyId = this._getCompanyId(companyName);

        let offer = {
            company: {
                id: companyId,
                name: companyName
            },
            buyOffers: [],
            sellOffers: []
        };

        this._offers.push(offer);

        return offer;
    },
    _unpersistCompany: function (companyId) {
        let self = this;

        let index = this._offers.findIndex(offer => offer.company.id === companyId);

        if (index > -1) {
            let company = this._offers.splice(index, 1)[0];

            company.buyOffers.forEach(offer => {
                localStorage.removeItem(offer.id);
                self._unsetBuyOfferAsCurrent(offer.id);
            });

            company.sellOffers.forEach(offer => {
                localStorage.removeItem(offer.id);
                self._unsetSellOfferAsCurrent(offer.id);
            });
        }
    },
    _persistBuyOffer: function (rateType, companyId, companyName, date, periods, dto) {
        let offer = {
            company: {
                id: companyId,
                name: companyName
            },
            date: date,
            rateType: rateType,
            periods: periods,
            rawPeriods: periods,
            dto: dto
        }

        if (dto) {
            finalDto = dto.split('>').map(si => parseInt(si.trim()) / 100).reduce((a, b) => a * (1 - b), 1);

            offer['periods'] = periods.map(period => {
                return [period[0], Math.round((period[1] * finalDto) * 100000) / 100000]
            })
        }

        let offerId = `buy_offer_${this._hash(JSON.stringify(offer))}`;

        offer['id'] = offerId;

        localStorage.setItem(offer.id, JSON.stringify(offer));

        this._offers.filter(c => c.company.id === companyId).forEach(c => c.buyOffers.push(offer));

        return offer;
    },
    _unpersistBuyOffer: function (companyId, offerId) {
        localStorage.removeItem(offerId);

        let company = this._offers.find(offer => offer.company.id === companyId)

        let offerIndex = company.buyOffers.findIndex(offer => offer.id === offerId);

        if (offerIndex !== -1) {
            company.buyOffers.splice(offerIndex, 1);

            this._unsetBuyOfferAsCurrent(offerId);
        }
    },
    _persistSellOffer: function (companyId, companyName, date, price) {
        let offer = {
            company: {
                id: companyId,
                name: companyName
            },
            date: date,
            price: price
        }

        let offerId = `sell_offer_${this._hash(JSON.stringify(offer))}`;

        offer['id'] = offerId;

        localStorage.setItem(offer.id, JSON.stringify(offer));

        this._offers.filter(c => c.company.id === companyId).forEach(c => c.sellOffers.push(offer));

        return offer;
    },
    _unpersistSellOffer: function (companyId, offerId) {
        localStorage.removeItem(offerId);

        let company = this._offers.find(offer => offer.company.id === companyId)

        let offerIndex = company.sellOffers.findIndex(offer => offer.id === offerId);

        if (offerIndex !== -1) {
            company.sellOffers.splice(offerIndex, 1);

            this._unsetSellOfferAsCurrent(offerId);
        }
    },

    _buyOfferIsCurrent: function (offerId) {
        return localStorage.getItem('current_buy_offer') === offerId;
    },
    _setBuyOfferAsCurrent: function (offerId) {
        localStorage.setItem('current_buy_offer', offerId);
    },
    _unsetBuyOfferAsCurrent: function (offerId) {
        if (this._buyOfferIsCurrent(offerId)) {
            localStorage.removeItem('current_buy_offer');
        }
    },
    _sellOfferIsCurrent: function (offerId) {
        return localStorage.getItem('current_sell_offer') === offerId;
    },
    _setSellOfferAsCurrent: function (offerId) {
        localStorage.setItem('current_sell_offer', offerId);
    },
    _unsetSellOfferAsCurrent: function (offerId) {
        if (this._sellOfferIsCurrent(offerId)) {
            localStorage.removeItem('current_sell_offer');
        }
    }
});
})();