document.title = document.title + ': {{ title }}';
title_meta = $('meta[property="og:title"]')
title_meta.attr('content', title_meta.attr('content') + ': {{ title }}');
