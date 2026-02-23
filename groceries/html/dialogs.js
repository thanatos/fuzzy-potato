const PRELOADED_X = (function() {
	const x_img = document.createElement('img');
	x_img.setAttribute('src', '/images/icons/fa/xmark.svg');
	return x_img;
})();

export function modalDialog(body_frag, buttons) {
	function div() { return document.createElement('div'); }

	const promise_pkg = Promise.withResolvers();
	const dialog = document.createElement('dialog');
	dialog.setAttribute('class', 'generic-dialog');

	const vert_layout_div = div();

	const x_div = div();
	x_div.setAttribute('class', 'dialog-x');
	const x_img = PRELOADED_X.cloneNode();
	x_img.setAttribute('class', 'icon-btn');
	x_img.onclick = () => {
		dialog.remove();
		promise_pkg.resolve({
			event: 'closed',
		});
	};
	x_div.appendChild(x_img);
	vert_layout_div.appendChild(x_div);

	const content_div = div();
	content_div.appendChild(body_frag);
	vert_layout_div.appendChild(content_div);

	const buttons_div = div();
	buttons_div.setAttribute('class', 'buttons');
	for(const button of buttons) {
		const button_el = document.createElement('button');
		button_el.textContent = button.text;
		button_el.onclick = () => {
			dialog.remove();
			promise_pkg.resolve({
				event: 'button',
				signal: button.signal,
			});
		};
		buttons_div.appendChild(button_el);
	}
	vert_layout_div.appendChild(buttons_div);
	
	dialog.appendChild(vert_layout_div);
	document.body.appendChild(dialog);
	dialog.showModal();
	return promise_pkg.promise;
}
