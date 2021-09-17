import logging
import os

logger = logging.getLogger("qondor")


def cmsconnect_get_all_sites():
    """
    Reads the central config for cmsconnect to determine the list of all available sites
    """
    try:
        from configparser import RawConfigParser  # python 3
    except ImportError:
        import ConfigParser  # python 2

        RawConfigParser = ConfigParser.RawConfigParser
    cfg = RawConfigParser()
    cfg.read("/etc/ciconnect/config.ini")
    sites = cfg.get("submit", "DefaultSites").split(",")
    all_sites = set(sites)
    return all_sites


def cmsconnect_settings(sub, blacklist=None, whitelist=None, cli=False):
    """
    Adds special cmsconnect settings to submission dict in order to submit
    on cmsconnect via the python bindings, or in case of submitting by the
    command line, to set the DESIRED_Sites key.
    Modifies the dict in place.
    """
    all_sites = cmsconnect_get_all_sites()

    # Check whether the user whitelisted or blacklisted some sites
    desired_sites = None
    if blacklist or whitelist:
        import fnmatch

        blacklisted = []
        whitelisted = []
        # Build the blacklist
        if blacklist:
            for blacksite_pattern in blacklist:
                for site in all_sites:
                    if fnmatch.fnmatch(site, blacksite_pattern):
                        blacklisted.append(site)
        # Build the whitelist
        if whitelist:
            for whitesite_pattern in whitelist:
                for site in all_sites:
                    if fnmatch.fnmatch(site, whitesite_pattern):
                        whitelisted.append(site)
        # Convert to list and sort
        blacklisted = list(set(blacklisted))
        blacklisted.sort()
        whitelisted = list(set(whitelisted))
        whitelisted.sort()
        logger.info("Blacklisting: %s", ",".join(blacklisted))
        logger.info("Whitelisting: %s", ",".join(whitelisted))
        desired_sites = list(
            (set(all_sites) - set(blacklisted)).union(set(whitelisted))
        )
        desired_sites.sort()

    # Add a plus only if submitting via .jdl file
    def addplus(key):
        return "+" + key if cli else key

    if desired_sites:
        logger.info("Submitting to desired sites: %s", ",".join(desired_sites))
        sub[addplus("DESIRED_Sites")] = '"' + ",".join(desired_sites) + '"'
    else:
        logger.info("Submitting to all sites: %s", ",".join(all_sites))
    if not cli:
        sub[addplus("ConnectWrapper")] = '"2.0"'
        sub[addplus("CMSGroups")] = '"/cms,T3_US_FNALLPC"'
        sub[addplus("MaxWallTimeMins")] = "500"
        sub[addplus("ProjectName")] = '"cms.org.fnal"'
        sub[addplus("SubmitFile")] = '"irrelevant.jdl"'
        sub[addplus("AccountingGroup")] = '"analysis.{0}"'.format(os.environ["USER"])
        logger.warning(
            "FIXME: CMS Connect settings currently hard-coded for a FNAL user"
        )
