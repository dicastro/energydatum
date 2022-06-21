(function () {
$.widget('ed.productionestimationconfigurationselector', {
    options: {
        loadingContainerSelector: '',
        dataOrigin: {
            contextpath: '',
            dateScope: '',
        },
        configurationList: []
    },

    _htmlTemplate: '<div class="ui form">' +
        '<div class="fields">' +
            '<div class="ui calendar six wide field">' +
                '<label>Configuración</label>' +
                '<select name="configuration" class="ui search dropdown">' +
                    '<option value="">(Seleccione una configuración)</option>' +
                '</select>' +
            '</div>' +
            '<div class="two wide field">' +
                '<label>&nbsp;</label>' +
                '<div class="ui icon green button configuration-add">' +
                    '<i class="plus icon"></i>' +
                '</div>' +
            '</div>' +
        '</div>' +
        '<div id="selected-configurations" class="ui segment">' +
            '<p class="nocontent">No se ha seleccionado ninguna configuración</p>' +
        '</div>' +
    '</div>',

    _components: {},
    _configurations: [],

    _containsConfiguration: function(configurationId) {
        return this._configurations.some(function(configuration) {
            return configuration.id === configurationId;
        });
    },
    _hasConfigurations: function() {
        return this._configurations.length > 0;
    },
    _getConfigurationKey: function(configurationId) {
        return `production_estimation_configuration_${configurationId}`
    },
    _addConfiguration: function(configuration) {
        this._configurations.push(configuration);

        sessionStorage.setItem(this._getConfigurationKey(configuration.id), JSON.stringify(configuration));
    },
    _removeConfiguration: function(id) {
        let index = this._configurations.findIndex(function(configuration) {
            return configuration.id == id;
        });

        if (index > -1) {
            this._configurations.splice(index, 1);
        }

        sessionStorage.removeItem(this._getConfigurationKey(id));
    },

    _create: function () {
        let self = this;

        this.element.html(this._htmlTemplate);

        $configurationSelect = this.element.find('select[name="configuration"]');

        this._components['configurationSelect'] = $configurationSelect;

        this.options.configurationList.forEach(function (configuration) {
            $configurationSelect.append('<option value="' + configuration.id + '">' + configuration.label + '</option>');
        });

        $configurationSelect.dropdown({
            fullTextSearch: true,
            match: 'text',
            forceSelection: false,
            clearable: true,
        });

        this._bindAddEvent();
        this._bindRemoveEvent();
        this._loadConfigurations();

        if (this._hasConfigurations()) {
            this._configurations.forEach(configuration => self._addSelectedConfiguration(configuration));
            this._trigger('initialized', null, [this._configurations]);
        }
    },

    _setOption: function (key, value) {
        this.options[key] = value;
    },

    _getDataUrl: function(configurationId) {
        return `${this.options.dataOrigin.contextpath}/data/selfsupply/production_estimation/${this.options.dataOrigin.dateScope}/production_estimation_${configurationId}.json`;
    },

    _loadConfigurations: function() {
        this._configurations = Object.keys(sessionStorage)
            .filter(key => key.match(/production_estimation_configuration_/))
            .map(key => JSON.parse(sessionStorage.getItem(key)));
    },

    _bindAddEvent: function() {
        let self = this;

        this.element.find('.configuration-add').click(handler => {
            $configurationElement = self._components['configurationSelect'];
            let selectedConfigurationId = $configurationElement.val();

            if (!selectedConfigurationId) {
                $configurationElement.parents('div.field').effect('shake', { times: 4, distance: 8 }, 750);
            } else {
                if (!self._containsConfiguration(selectedConfigurationId)) {
                    $(this.options.loadingContainerSelector).prepend('<div id="configuration-estimation-loading" class="ui active dimmer"><div class="ui large text loader">Cargando ...</div></div>');

                    $.get(self._getDataUrl(selectedConfigurationId), function(selectedConfiguration) {
                        self._addConfiguration(selectedConfiguration);

                        self._addSelectedConfiguration(selectedConfiguration);

                        self._trigger('configurationAdded', null, selectedConfiguration);

                        $('#configuration-estimation-loading').remove();
                    });
                }

                $configurationElement.dropdown('clear', true);
            }
        });
    },
    _bindRemoveEvent: function() {
        let self = this;

        this.element.on('click', '.configuration-remove', handler => {
            let $configurationElement = $(handler.currentTarget).parents('.selected-configuration');
            let configurationId = $configurationElement.data('id');

            $configurationElement.remove();

            self._removeConfiguration(configurationId);

            self._trigger('configurationRemoved', null, configurationId);

            if (!self._hasConfigurations()) {
                self.element.find('#selected-configurations').append($('<p class="nocontent">').text('No se ha seleccionado ninguna configuración'));
            }
        });
    },

    _addSelectedConfiguration: function(configurationData) {
        let $selectedConfigurations = this.element.find('#selected-configurations');

        $selectedConfigurations.find('.nocontent').remove();
        $selectedConfigurations.append($('<div>', {
            class: 'ui left icon label selected-configuration',
            style: 'color: #ffffff; background-color: gray;',
            'data-id': `${configurationData.id}`,
            html: `<i class="close icon configuration-remove"></i>${configurationData.name}`
        }));
    }
});
})();