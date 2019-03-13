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
import threading

from linkTaskManager import LinkTaskManager
import jobChain
from utils import log_exceptions

choicesAvailableForUnits = {}
choicesAvailableForUnitsLock = threading.Lock()

from databaseFunctions import auto_close_db
from workflow_abilities import choice_is_available

from main.models import UserProfile, Job
from django.conf import settings as django_settings
from django.utils.six import text_type

LOGGER = logging.getLogger("archivematica.mcp.server")


class linkTaskManagerChoice(LinkTaskManager):
    """Used to get a selection, from a list of chains, to process"""

    def __init__(self, jobChainLink, unit):
        super(linkTaskManagerChoice, self).__init__(jobChainLink, unit)

        self._populate_choices()

        chain_id = self.checkForPreconfiguredXML()
        if chain_id is not None:
            self.jobChainLink.setExitMessage(Job.STATUS_COMPLETED_SUCCESSFULLY)
            chain = self.jobChainLink.workflow.get_chain(chain_id)
            jobChain.jobChain(self.unit, chain, jobChainLink.workflow)
            return

        choicesAvailableForUnitsLock.acquire()
        self.jobChainLink.setExitMessage(Job.STATUS_AWAITING_DECISION)
        choicesAvailableForUnits[self.jobChainLink.UUID] = self
        choicesAvailableForUnitsLock.release()

    def _populate_choices(self):
        self.choices = []
        for chain_id in self.jobChainLink.link.config["chain_choices"]:
            try:
                chain = self.jobChainLink.workflow.get_chain(chain_id)
            except KeyError:
                continue
            if not choice_is_available(self.jobChainLink.link, chain, django_settings):
                continue
            self.choices.append((chain_id, chain["description"], None))

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
            tree = etree.parse(xmlFilePath)
            root = tree.getroot()
            for preconfiguredChoice in root.findall(".//preconfiguredChoice"):
                if preconfiguredChoice.find("appliesTo").text != self.jobChainLink.pk:
                    continue
                ret = preconfiguredChoice.find("goToChain").text
                break
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
        for id_, description, __ in self.choices:
            choice = etree.SubElement(choices, "choice")
            etree.SubElement(choice, "chainAvailable").text = id_
            etree.SubElement(choice, "description").text = text_type(description)
        return ret

    @log_exceptions
    @auto_close_db
    def proceedWithChoice(self, chain_id, user_id):
        if user_id is not None:
            agent_id = UserProfile.objects.get(user_id=int(user_id)).agent_id
            agent_id = str(agent_id)
            self.unit.setVariable("activeAgent", agent_id, None)

        choicesAvailableForUnitsLock.acquire()
        del choicesAvailableForUnits[self.jobChainLink.UUID]
        choicesAvailableForUnitsLock.release()

        self.jobChainLink.setExitMessage(Job.STATUS_COMPLETED_SUCCESSFULLY)
        LOGGER.info("Using user selected chain %s", chain_id)
        chain = self.jobChainLink.workflow.get_chain(chain_id)
        jobChain.jobChain(self.unit, chain, self.jobChainLink.workflow)
