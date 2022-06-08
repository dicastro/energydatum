$(document).ready(function() {
    // lazy load images
    $('.image').visibility({
        type: 'image',
        transition: 'vertical flip in',
        duration: 500
    });

    $('.ui.accordion').accordion();

    $('.ui.modal').each((i, elem) => {
        triggerButton = $(elem).data('trigger-button');

        $(`#${triggerButton}`).click(handler => {
            modalId = $(handler.currentTarget).data('modal-id');
            $(`#${modalId}`).modal('show');
        });
    });

    $('.download-comp').click(handler => {
        handler.preventDefault();
        $element = $(handler.currentTarget);
        let fileUrl = $element.attr('href');
        let fileName = $element.data('filename');

        if ('Blob' in window) {
            $.get(fileUrl).done(response => {
                var blob = new Blob([JSON.stringify(response)], {type: 'application/json'});

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
            });
        } else {
            alert('Your browser does not support the HTML5 Blob.');
        }
    });
});