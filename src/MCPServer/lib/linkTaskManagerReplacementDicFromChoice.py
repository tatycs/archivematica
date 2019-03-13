# This file is part of Archivematica.
#
# Copyright 2010-2013 Artefactual Systems Inc. <http://artefactual.com>
#
# Archivematica is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Archivematica is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Archivematica.  If not, see <http://www.gnu.org/licenses/>.

# @package Archivematica
# @subpackage MCPServer
# @author Joseph Perry <joseph@artefactual.com>

import logging
import lxml.etree as etree
import os

from utils import choice_unifier
from linkTaskManager import LinkTaskManager
from linkTaskManagerChoice import choicesAvailableForUnits, choicesAvailableForUnitsLock

from dicts import ReplacementDict
from main.models import DashboardSetting, Job, UserProfile
from django.conf import settings as django_settings
from django.utils.six import text_type

LOGGER = logging.getLogger("archivematica.mcp.server")


class linkTaskManagerReplacementDicFromChoice(LinkTaskManager):
    def __init__(self, jobChainLink, unit):
        super(linkTaskManagerReplacementDicFromChoice, self).__init__(
            jobChainLink, unit
        )

        self.replacements = self.jobChainLink.link.config["replacements"]
        self._populate_choices()

        # There are MicroServiceChoiceReplacementDic links with no
        # replacements (``self.choices`` has zero elements at this point). This
        # is true for the following chain links:
        #
        #   - ``Choose Config for Archivists Toolkit DIP Upload``
        #   - ``Choose config for AtoM DIP upload``
        #   - ``Choose Config for ArchivesSpace DIP Upload``
        #
        # The only purpose of these links is to  load settings from the
        # Dashboard configuration store (``DashboardSetting``), e.g.
        # connection details or credentials that are needed to perform the
        # upload of the DIP to the remote system.
        #
        # Once the settings are loaded, we proceed with the next chain link
        # automatically instead of prompting the user with a single choice
        # which was considered inconvenient and confusing. In the future, it
        # should be possible to prompt the user only if we want to have the
        # user decide between multiple configurations, e.g. more than one
        # AtoM instance is available and the user wants to decide which one is
        # going to be used.
        rdict = self._get_dashboard_setting_choice()
        if rdict and not self.choices:
            LOGGER.debug("Found Dashboard settings for this task, proceed.")
            self.update_passvar_replacement_dict(rdict)
            self.jobChainLink.linkProcessingComplete(
                0, passVar=self.jobChainLink.passVar
            )
            return

        preConfiguredChain = self.checkForPreconfiguredXML()
        if preConfiguredChain is not None:
            self.jobChainLink.setExitMessage(Job.STATUS_COMPLETED_SUCCESSFULLY)
            rd = ReplacementDict(preConfiguredChain)
            self.update_passvar_replacement_dict(rd)
            self.jobChainLink.linkProcessingComplete(
                0, passVar=self.jobChainLink.passVar
            )
            return

        choicesAvailableForUnitsLock.acquire()
        self.jobChainLink.setExitMessage(Job.STATUS_AWAITING_DECISION)
        choicesAvailableForUnits[self.jobChainLink.UUID] = self
        choicesAvailableForUnitsLock.release()

    def _format_items(self, items):
        """Wrap replacement items with the ``%`` wildcard character."""
        return {"%{}%".format(key): value for key, value in items.items()}

    def _populate_choices(self):
        self.choices = []
        for index, item in enumerate(self.replacements):
            self.choices.append(
                (index, item["description"], self._format_items(item["items"]))
            )

    def _get_dashboard_setting_choice(self):
        """Load settings associated to this task into a ``ReplacementDict``.

        The model used (``DashboardSetting``) is a shared model.
        """
        try:
            link = self.jobChainLink.workflow.get_link(
                self.jobChainLink.link["fallback_link_id"]
            )
        except KeyError:
            return
        execute = link.config["execute"]
        if not execute:
            return
        args = DashboardSetting.objects.get_dict(execute)
        if not args:
            return
        return ReplacementDict(self._format_items(args))

    def checkForPreconfiguredXML(self):
        ret = None
        xmlFilePath = os.path.join(
            self.unit.currentPath.replace(
                "%sharedPath%", django_settings.SHARED_DIRECTORY, 1
            ),
            django_settings.PROCESSING_XML_FILE,
        )
        if not os.path.isfile(xmlFilePath):
            return None
        try:
            this_choice_point = choice_unifier.get(
                self.jobChainLink.pk, self.jobChainLink.pk
            )
            tree = etree.parse(xmlFilePath)
            root = tree.getroot()
            for preconfiguredChoice in root.findall(".//preconfiguredChoice"):
                if preconfiguredChoice.find("appliesTo").text != this_choice_point:
                    continue
                desiredChoice = preconfiguredChoice.find("goToChain").text
                desiredChoice = choice_unifier.get(desiredChoice, desiredChoice)
                try:
                    link = self.jobChainLink.workflow.get_link(this_choice_point)
                except KeyError:
                    return
                for replacement in link.config["replacements"]:
                    if replacement["id"] == desiredChoice:
                        # In our JSON-encoded document, the items in
                        # the replacements are not wrapped, do it here.
                        # Needed by ReplacementDict.
                        ret = self._format_items(replacement["items"])
                        break
                else:
                    return
        except Exception:
            LOGGER.warning(
                "Error parsing xml at %s for pre-configured choice",
                xmlFilePath,
                exc_info=True,
            )
        return ret

    def xmlify(self):
        """Returns an etree XML representation of the choices available."""
        ret = etree.Element("choicesAvailableForUnit")
        etree.SubElement(ret, "UUID").text = self.jobChainLink.UUID
        ret.append(self.unit.xmlify())
        choices = etree.SubElement(ret, "choices")
        for index, description, __ in self.choices:
            choice = etree.SubElement(choices, "choice")
            etree.SubElement(choice, "chainAvailable").text = text_type(index)
            etree.SubElement(choice, "description").text = text_type(description)
        return ret

    def proceedWithChoice(self, index, user_id):
        if user_id:
            agent_id = UserProfile.objects.get(user_id=int(user_id)).agent_id
            agent_id = str(agent_id)
            self.unit.setVariable("activeAgent", agent_id, None)

        choicesAvailableForUnitsLock.acquire()
        del choicesAvailableForUnits[self.jobChainLink.UUID]
        choicesAvailableForUnitsLock.release()

        # get the one at index, and go with it.
        __, __, items = self.choices[int(index)]
        self.update_passvar_replacement_dict(ReplacementDict(items))
        self.jobChainLink.linkProcessingComplete(0, passVar=self.jobChainLink.passVar)
