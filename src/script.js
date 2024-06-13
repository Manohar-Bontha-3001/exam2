document.addEventListener('DOMContentLoaded', function () {
    const forms = document.querySelectorAll('form');

    forms.forEach(form => {
        form.addEventListener('submit', function (event) {
            const inputs = form.querySelectorAll('input[type="text"]');
            let valid = true;

            inputs.forEach(input => {
                if (input.value.trim() === '') {
                    input.style.borderColor = 'red';
                    valid = false;
                } else {
                    input.style.borderColor = '#ccc';
                }
            });

            if (!valid) {
                event.preventDefault();
                alert('Please fill in all required fields.');
            }
        });
    });
});
