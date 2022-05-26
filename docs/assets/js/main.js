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
});