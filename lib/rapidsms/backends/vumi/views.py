import json
import logging

from django.http import HttpResponse

from rapidsms.backends.vumi.forms import VumiForm
from rapidsms.backends.newhttp.views import BaseHttpBackendView


logger = logging.getLogger(__name__)


class VumiBackendView(BaseHttpBackendView):
    """
    Backend view for handling inbound SMSes from Vumi (http://vumi.org/)
    """

    http_method_names = ['post']
    form_class = VumiForm

    def get_form_kwargs(self):
        """Load JSON POST data."""
        kwargs = super(VumiBackendView, self).get_form_kwargs()
        print self.request
        try:
            json_String = self.request.POST.items()[0][0]
            kwargs['data'] = json.loads(json_String)
            logger.error(self.request.POST.items())
        except ValueError:
            logger.exception("Failed to parse JSON from Vumi.")
        return kwargs

    def form_valid(self, form):
        super(VumiBackendView, self).form_valid(form)
        # return 200 for Vumi
        return HttpResponse('')
