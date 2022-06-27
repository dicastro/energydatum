offerdb = {
    _getBuyOfferIds: function () {
        return Object.keys(localStorage).filter(key => key.match(/buy_offer_\d+/));
    },
    _getSellOfferIds: function () {
        return Object.keys(localStorage).filter(key => key.match(/sell_offer_\d+/));
    },

    _getOfferRaw: function (offerId) {
        return localStorage.getItem(offerId);
    },

    _getOffer: function (offerId) {
        return JSON.parse(this._getOfferRaw(offerId));
    },

    getAllBuyOffers: function () {
        let self = this;
        return this._getBuyOfferIds().map(offerId => self._getOffer(offerId));
    },
    getAllSellOffers: function () {
        let self = this;
        return this._getSellOfferIds().map(offerId => self._getOffer(offerId));
    },

    getBuyOfferRateTypes: function () {
        let self = this;
        return this._getBuyOfferIds().map(offerId => self._getOffer(offerId).rateType).filter((v, i, acc) => acc.indexOf(v) === i);
    },

    getCurrentBuyOffer: function () {
        let cboId = localStorage.getItem('current_buy_offer');

        let cbo;

        if (cboId) {
            cbo = this._getOffer(cboId);
        }

        return cbo;
    },
    getCurrentSellOffer: function () {
        let csoId = localStorage.getItem('current_sell_offer');

        let cso;

        if (csoId) {
            cso = this._getOffer(csoId);
        }

        return cso;
    },
}