# -*- coding: utf-8 -*-

"""
sceptre.config

This module implements a Config class, which stores a stack or environment's
configuration.
"""

from glob import glob
import copy
import collections
import datetime
import logging
import os
from pkg_resources import iter_entry_points
from functools import partial
import yaml

import jinja2
from packaging.specifiers import SpecifierSet
from packaging.version import Version

from . import __version__
from .exceptions import EnvironmentPathNotFoundError
from .exceptions import VersionIncompatibleError
from .environment import Environment
from .stack import Stack
from .helpers import get_external_stack_name

ConfigAttributes = collections.namedtuple("Attributes", "required optional")

ENVIRONMENT_CONFIG_ATTRIBUTES = ConfigAttributes(
    {
        "project_code",
        "region"
    },
    {
        "iam_role",
        "template_bucket_name",
        "template_key_prefix",
        "require_version"
    }
)

STACK_CONFIG_ATTRIBUTES = ConfigAttributes(
    {
        "template_path"
    },
    {
        "dependencies",
        "hooks",
        "parameters",
        "protect",
        "sceptre_user_data",
        "stack_name",
        "stack_tags",
        "role_arn"
    }
)


class ConfigReader(object):
    """
    Config stores the configuration read in from the YAML files. Config
    inherits from dict, and so configuration data can be accessed from Config
    as if Config were a dict. Config implements read(), a method to read in
    config from the ``<base_file_name>.yaml`` files along the
    ``environment_path`` from ``sceptre_dir``.

    :param sceptre_dir: The absolute path to the Sceptre directory.
    :type project dir: str
    :param environment_path: The name of the environment.
    :type environment_path: str
    :param base_file_name: The basename of the file to read in \
        (e.g. "config", "vpc")
    :type base_file_name: str
    """

    def __init__(self, sceptre_dir):
        self.logger = logging.getLogger(__name__)
        self._deferred_constructors = []

        self.sceptre_dir = sceptre_dir
        self.config_folder = os.path.join(self.sceptre_dir, "config")
        self._check_env_path_exists(self.config_folder)

        self._add_yaml_constructors(
            ["sceptre.hooks", "sceptre.resolvers"]
        )

    def _add_yaml_constructors(self, entry_points):
        """
        Adds PyYAML constructors for all classes found registered at the
        entry_point_name.

        :param entry_point_name: The name of the entry point.
        :type entry_point_name: str
        :param environment_config: The environment config.
        :type environment_config: Config
        """
        self.logger.debug("Adding yaml constructors")

        def factory(node_class):
            """
            This returns a lambda function that will contruct objects from a
            given node class.

            :param node_class: A class to construct of objects from.
            :type node_class: class
            :returns: A lambda that constructs objects.
            :rtype: func
            """
            def class_constructor(loader, node):
                return partial(node_class, loader.construct_scalar(node))

            return class_constructor

        for entry_point_name in entry_points:
            for entry_point in iter_entry_points(entry_point_name):
                node_tag = u'!' + entry_point.name
                node_class = entry_point.load()
                yaml.SafeLoader.add_constructor(
                    node_tag, factory(node_class)
                )
                self.logger.debug(
                    "Added constructor for %s with node tag %s",
                    str(node_class), node_tag
                )

    def read(self, rel_path, base_config={}):
        """
        Reads in configuration from files.

        Traverses the environment path, from top to bottom, reading in all
        relevant config files. If config items appear in files lower down the
        environment tree, they overwrite items from further up. Jinja2 is used
        to template in variables from user_variables, environment variables,
        and the segments of the environment path.

        :param user_variables: A dict of key value pairs to be supplied to \
        the config file via Jinja2 templating.
        :type user_variables: dict
        """
        self.logger.debug("Reading in '%s' files...", rel_path)

        config = {
            "sceptre_dir": self.sceptre_dir,
            "environment_path": os.path.split(rel_path)[0]
        }
        config.update(base_config)

        if (
            not os.path.isfile(os.path.join(self.config_folder, rel_path)) and
            not rel_path.endswith("config.yaml")
        ):
            print(os.path.join(self.config_folder, rel_path))
            raise Exception("Config does not exist.")

        directory_path, basename = os.path.split(rel_path)

        config.update(self._recursive_read(directory_path, basename))
        self.construct_nodes(config, config)

        self._check_version(config)

        self.logger.debug("Config: %s", config)
        return config

    def _recursive_read(self, directory_path, basename):
        parent_directory = os.path.split(directory_path)[0]
        if not directory_path:
            return self._read(directory_path, basename) or {}

        config = self._recursive_read(parent_directory, basename)
        child_config = self._read(directory_path, basename) or {}
        config.update(child_config)

        return config

    def _read(self, directory_path, basename):
        """
        Recursively reads in config files in nested subfolders.
        """
        directory_path = os.path.join(self.config_folder, directory_path)
        if os.path.isfile(os.path.join(directory_path, basename)):
            env = jinja2.Environment(
                loader=jinja2.FileSystemLoader(directory_path),
                undefined=jinja2.StrictUndefined
            )
            template = env.get_template(basename)
            rendered_template = template.render(
                environment_variable=os.environ
            )
            config = yaml.safe_load(rendered_template)
            return config

    def construct_nodes(self, attr, config):
        if isinstance(attr, dict):
            for key, value in attr.items():
                if isinstance(value, partial):
                    attr[key] = value(config)
                elif isinstance(value, list) or isinstance(value, dict):
                    self.construct_nodes(value, config)
        elif isinstance(attr, list):
            for index, value in enumerate(attr):
                if isinstance(value, partial):
                    attr[index] = value(config)
                elif isinstance(value, list) or isinstance(value, dict):
                    self.construct_nodes(value, config)

    @staticmethod
    def _check_env_path_exists(path):
        """
        Raises an EnvironmentPathNotFoundError if ``path`` is not a directory.

        :param path: A directory path.
        :type path: str
        :raises: sceptre.exceptions.EnvironmentPathNotFoundError
        """
        if not os.path.isdir(path):
            raise EnvironmentPathNotFoundError(
                "The environment '{0}' does not exist.".format(path)
            )

    def _check_version(self, config):
        """
        Raises a VersionIncompatibleException when the current sceptre version
        does not comply with the configured version requirement.

        :raises: sceptre.exceptions.VersionIncompatibleException
        """
        sceptre_version = __version__
        if 'require_version' in config:
            require_version = config['require_version']
            if Version(sceptre_version) not in SpecifierSet(require_version):
                raise VersionIncompatibleError(
                    "Current sceptre version ({0}) does not meet version "
                    "requirements: {1}".format(
                        sceptre_version, require_version
                    )
                )

    def _construct_stack(self, rel_path, config={}):
        directory, basename = os.path.split(rel_path)
        if basename != "config.yaml":
            config = self.read(rel_path, config)
            stack_name = os.path.splitext(rel_path)[0]
            external_name = config.get(
                "stack_name",
                get_external_stack_name(config["project_code"], stack_name)
            )
            abs_template_path = os.path.join(
                self.sceptre_dir, config["template_path"]
            )

            s3_details = None
            if "template_bucket_name" in config:
                template_key = "/".join([
                    external_name, "{time_stamp}.json".format(
                        time_stamp=datetime.datetime.utcnow().strftime(
                            "%Y-%m-%d-%H-%M-%S-%fZ"
                        )
                    )
                ])

                if "template_key_prefix" in config:
                    prefix = config["template_key_prefix"]
                    template_key = "/".join([prefix.strip("/"), template_key])

                s3_details = {
                     "bucket_name": config["template_bucket_name"],
                     "bucket_key": template_key,
                     "region": config["region"]
                }

            stack = Stack(
                name=stack_name, external_name=external_name,
                template_path=abs_template_path,
                iam_role=config.get("iam_role"), s3_details=s3_details,
                role_arn=config.get("role_arn"),
                tags=config.get("stack_tags", {}),
                protected=config.get("protect"), region=config["region"],
                dependencies=config.get("dependencies", []),
                sceptre_user_data=config.get("sceptre_user_data", {}),
                parameters=config.get("parameters", {})
            )
            return stack

    def construct_stack(self, rel_path):
        directory = os.path.split(rel_path)[0]
        environment_config = self.read(os.path.join(directory, "config.yaml"))
        return self._construct_stack(rel_path, environment_config)

    def construct_environment(self, rel_path):
        environment_config = self.read(os.path.join(rel_path, "config.yaml"))
        environment = Environment(rel_path)

        items = glob(
            os.path.join(self.sceptre_dir, "config", rel_path, "*")
        )

        paths = {
            item: os.path.relpath(
                item, os.path.join(self.sceptre_dir, "config")
            )
            for item in items if not item.endswith("config.yaml")
        }

        for abs_path, rel_path in paths.items():
            if os.path.isdir(abs_path):
                environment.environments.append(
                    self.construct_environment(rel_path)
                )
            elif os.path.isfile(abs_path):
                stack = self._construct_stack(
                    rel_path, copy.deepcopy(environment_config)
                )
                environment.stacks.append(stack)

        return environment
