offerdb = {
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

    _isId: function (id) {
        return id.match(/consumption_offer_\d+/);
    },

    getConsumptionOfferIds: function () {
        that = this;
        return Object.keys(localStorage).filter(key => that._isId(key));
    },

    saveConsumptionOffer: function (company, date, rateType, periods, dto) {
        let offer = {
            'company': company,
            'date': date,
            'rateType': rateType,
            'periods': periods,
            'rawPeriods': periods,
            'dto': dto
        }

        if (dto) {
            finalDto = dto.split('>').map(si => parseInt(si.trim()) / 100).reduce((a, b) => a * (1 - b), 1);

            offer['periods'] = periods.map(period => {
                return [period[0], Math.round((period[1] * finalDto) * 100000) / 100000]
            })
        }

        let offerId = `consumption_offer_${this._hash(JSON.stringify(offer))}`;

        offer['id'] = offerId;

        localStorage.setItem(offer.id, JSON.stringify(offer));

        return offer;
    },

    getConsumptionOfferRaw: function (offerId) {
        return localStorage.getItem(offerId);
    },

    getConsumptionOffer: function (offerId) {
        return JSON.parse(this.getConsumptionOfferRaw(offerId));
    },

    getAllConsumptionOffers: function () {
        that = this;
        return this.getConsumptionOfferIds().map(offerId => that.getConsumptionOffer(offerId));
    },

    getConsumptionOffersByRateType: function (rateType) {
        return this.getAllConsumptionOffers().filter(offer => offer.rateType === rateType);
    },

    deleteConsumptionOffer: function (offerId) {
        localStorage.removeItem(offerId);

        if (this.consumptionOfferIsCurrent(offerId)) {
            this.unsetConsumptionOfferAsCurrent();
        }
    },

    deleteAllConsumptionOffers: function () {
        that = this;
        this.getConsumptionOfferIds().forEach(offerId => that.deleteConsumptionOffer(offerId));
    },

    getConsumptionOfferRateTypes: function () {
        that = this;
        return this.getConsumptionOfferIds().map(offerId => that.getConsumptionOffer(offerId).rateType).filter((v, i, acc) => acc.indexOf(v) === i);
    },

    setConsumptionOfferAsCurrent: function (offerId) {
        localStorage.setItem('consumption_offer_current', this.getConsumptionOfferRaw(offerId));
    },

    unsetConsumptionOfferAsCurrent: function () {
        localStorage.removeItem('consumption_offer_current');
    },

    getCurrentConsumptionOffer: function () {
        return this.getConsumptionOffer('consumption_offer_current');
    },

    consumptionOfferIsCurrent: function (offerId) {
        let currentConsumptionOffer = this.getCurrentConsumptionOffer();
        return currentConsumptionOffer && currentConsumptionOffer.id === offerId;
    },

    dump: function () {
        dump = {};

        this.getConsumptionOfferIds().forEach(offerId => {
            dump[offerId] = this.getConsumptionOffer(offerId);
        });

        dump['consumption_offer_current'] = this.getCurrentConsumptionOffer();

        return dump;
    },

    load: function (data) {
        that = this;
        this.deleteAllConsumptionOffers();

        Object.keys(data).forEach(key => {
            if (that._isId(key)) {
                this.saveConsumptionOffer(data[key].company, data[key].date, data[key].rateType, data[key].rawPeriods, data[key].dto);
            }
        });

        if (data['consumption_offer_current']) {
            this.setConsumptionOfferAsCurrent(data['consumption_offer_current'].id);
        }
    }
}