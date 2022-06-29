offerdb = {
    _parseDate: function (rawDate) {
        let day = rawDate.substring(0, 2);
        let month = rawDate.substring(3, 2);
        let year = rawDate.substring(6, 4);

        return new Date(year, month, day);
    },

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

    /**
    let ex = [{
        company: {
            id: '',
            name: '',
        },
        pairs: {
            '20td': {
                buyOffer: {},
                sellOffer: {},
            }
        }
    }]
    */
    getBuySellOfferPairs: function () {
        let buyOffers = this.getAllBuyOffers();

        let offerPairs = buyOffers.reduce((acc, offer) => {
            let companyIndex = acc.findIndex(c => c.company.id === offer.company.id);

            if (companyIndex === -1) {
                let pairs = {}
                pairs[offer.rateType] = {
                    buyOffer: offer
                }

                acc.push({
                    company: offer.company,
                    pairs: pairs,
                });
            } else {
                let existingPair = acc[companyIndex]['pairs'][offer.rateType];

                if (existingPair) {
                    let existingDate = this._parseDate(existingPair.buyOffer.date);
                    let newDate = this._parseDate(offer.date);

                    if (newDate > existingDate) {
                        acc[companyIndex]['pairs'][offer.rateType]['buyOffer'] = offer;
                    }
                } else {
                    acc[companyIndex]['pairs'][offer.rateType] = {
                        buyOffer: offer
                    }
                }
            }

            return acc;
        }, []);

        let sellOffers = this.getAllSellOffers();

        offerPairs = sellOffers.reduce((acc, offer) => {
            let companyIndex = acc.findIndex(c => c.company.id === offer.company.id);

            if (companyIndex !== -1) {
                Object.keys(acc[companyIndex]['pairs']).forEach(pairRateType => {
                    let existingPair = acc[companyIndex]['pairs'][pairRateType];

                    if (existingPair.sellOffer) {
                        let existingDate = this._parseDate(existingPair.sellOffer.date);
                        let newDate = this._parseDate(offer.date);

                        if (newDate > existingDate) {
                            acc[companyIndex]['pairs'][pairRateType]['sellOffer'] = offer;
                        }
                    } else {
                        existingPair['sellOffer'] = offer;
                    }
                });
            }

            return acc;
        }, offerPairs);

        return offerPairs
            .filter(offerPair => Object.values(offerPair.pairs).filter(pair => pair.buyOffer && pair.sellOffer).length > 0);
    }
}