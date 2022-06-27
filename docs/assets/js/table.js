$(document).ready(function() {
    $.fn.dataTable.ext.type.order['month-pre'] = function (d) {
        switch (d) {
            case 'Enero':      return 1;
            case 'Febrero':    return 2;
            case 'Marzo':      return 3;
            case 'Abril':      return 4;
            case 'Mayo':       return 5;
            case 'Junio':      return 6;
            case 'Julio':      return 7;
            case 'Agosto':     return 8;
            case 'Septiembre': return 9;
            case 'Octubre':    return 10;
            case 'Noviembre':  return 11;
            case 'Diciembre':  return 12;
        }

        return 0;
    };

    $.fn.dataTable.ext.type.order['dow-pre'] = function (d) {
        switch (d) {
            case 'Lunes': return 1;
            case 'Martes': return 2;
            case 'Miércoles': return 3;
            case 'Jueves': return 4;
            case 'Viernes': return 5;
            case 'Sábado': return 6;
            case 'Domingo': return 7;
        }

        return 0;
    };

    $.fn.dataTable.ext.type.order['month_year-pre'] = function (d) {
        var parts = d.split('-');

        return parts[1] + '' + parts[0];
    };

    $('.dt').each(function() {
        var $this = $(this);

        var column_defs = [];

        var order = [];

        $this.find('thead th').each(function(i) {
            var $th = $(this);
            var className = $th.attr('dt-className') || 'dt-left';
            var type = $th.attr('dt-type');
            var sorted = $th.attr('dt-sorted');

            column_def = {
                targets: i,
                className: className
            };

            if (type) {
                column_def['type'] = type;
            }

            if (sorted) {
                order.push([i, sorted]);
            }

            column_defs.push(column_def);
        });

        dataTableConfig = {
            'autoWidth': false,
            'columnDefs': column_defs,
            'order': order
        }

        pageSizes = $this.attr('dt-page-sizes')

        if (pageSizes) {
            lengthMenuValues = [];
            lengthMenuLiterals = [];

            pageSizes.split(',').forEach(function(pageSize) {
                val = parseInt(pageSize);

                if (val === -1) {
                    lengthMenuValues.push(val);
                    lengthMenuLiterals.push('Todos');
                } else {
                    lengthMenuValues.push(val);
                    lengthMenuLiterals.push(val);
                }
            });

            dataTableConfig['lengthMenu'] = [lengthMenuValues, lengthMenuLiterals];
        }

        dataUrl = $this.attr('dt-dataUrl')

        if (dataUrl) {
            dataTableConfig['ajax'] = {
                url: dataUrl,
                cache: true
            };
        }

        $this.DataTable(dataTableConfig);
    });
});