#
# GRAKN.AI - THE KNOWLEDGE GRAPH
# Copyright (C) 2018 Grakn Labs Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

workspace(name = "grakn_core")

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")


########################################
#        Remote Build Execution        #
########################################

http_archive(
  name = "bazel_toolchains",
  sha256 = "07a81ee03f5feae354c9f98c884e8e886914856fb2b6a63cba4619ef10aaaf0b",
  strip_prefix = "bazel-toolchains-31b5dc8c4e9c7fd3f5f4d04c6714f2ce87b126c1",
  urls = [
    "https://mirror.bazel.build/github.com/bazelbuild/bazel-toolchains/archive/31b5dc8c4e9c7fd3f5f4d04c6714f2ce87b126c1.tar.gz",
    "https://github.com/bazelbuild/bazel-toolchains/archive/31b5dc8c4e9c7fd3f5f4d04c6714f2ce87b126c1.tar.gz",
  ],
)


####################
# Load Build Tools #
####################

# Load additional build tools, such bazel-deps and unused-deps
load("//dependencies/tools:dependencies.bzl", "tools_dependencies")
tools_dependencies()


#####################################
# Load Java dependencies from Maven #
#####################################

load("//dependencies/maven:dependencies.bzl", maven_dependencies_for_build = "maven_dependencies")
maven_dependencies_for_build()


######################################
# Load Python dependencies from PyPI #
######################################

# Load Python depdendencies for Bazel
load("//dependencies/pip:dependencies.bzl", "python_dependencies",)
python_dependencies()

load("@io_bazel_rules_python//python:pip.bzl", "pip_repositories", "pip_import")
pip_repositories()

# Load PyPI dependencies for Python programs
pip_import(
    name = "pypi_dependencies",
    requirements = "//dependencies/pip:requirements.txt",
)
load("@pypi_dependencies//:requirements.bzl", "pip_install")
pip_install()


######################################
# Load Node.js dependencies from NPM #
######################################

# Load Node.js depdendencies for Bazel
load("//dependencies/npm:dependencies.bzl", "node_dependencies")
node_dependencies()

load("@build_bazel_rules_nodejs//:package.bzl", "rules_nodejs_dependencies")
rules_nodejs_dependencies()

# Load NPM dependencies for Node.js programs
load("@build_bazel_rules_nodejs//:defs.bzl", "node_repositories", "npm_install")
node_repositories(package_json = ["//client-nodejs:package.json"])
npm_install(
    name = "nodejs_dependencies",
    package_json = "//client-nodejs:package.json",
    data = [
      "@build_bazel_rules_nodejs//internal/babel_library:package.json",
      "@build_bazel_rules_nodejs//internal/babel_library:babel.js",
      "@build_bazel_rules_nodejs//internal/babel_library:yarn.lock",
    ],
)

node_repositories(package_json = ["//workbase:package.json"])

########################################
# Load compiler dependencies for ANTLR #
########################################

# Load ANTLR dependencies for Bazel
load("//dependencies/compilers:dependencies.bzl", "antlr_dependencies")
antlr_dependencies()

# Load ANTLR dependencies for ANTLR programs
load("@rules_antlr//antlr:deps.bzl", "antlr_dependencies")
antlr_dependencies()


#######################################
# Load compiler dependencies for GRPC #
#######################################

# Load GRPC dependencies
load("//dependencies/compilers:dependencies.bzl", "grpc_dependencies")
grpc_dependencies()

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", com_github_grpc_grpc_bazel_grpc_deps = "grpc_deps")
com_github_grpc_grpc_bazel_grpc_deps()

# Load GRPC Java dependencies
load("@stackb_rules_proto//java:deps.bzl", "java_grpc_compile")
java_grpc_compile()

# Load GRPC Python dependencies
load("@stackb_rules_proto//python:deps.bzl", "python_grpc_compile")
python_grpc_compile()

# Load GRPC Node.js dependencies
load("@stackb_rules_proto//node:deps.bzl", "node_grpc_compile")
node_grpc_compile()


########################################
#     Load Deployment Dependencies     #
########################################

git_repository(
    name="graknlabs_rules_deployment",
    remote="https://github.com/graknlabs/deployment",
    commit="784b332bfe0188989802a7e83e016be43c9e0168",
)

load("@graknlabs_rules_deployment//github:dependencies.bzl", "dependencies_for_github_deployment")
dependencies_for_github_deployment()


git_repository(
    name="com_github_google_bazel_common",
    remote="https://github.com/graknlabs/bazel-common",
    commit="550f0490798a4e4b6c5ff8cac3b6f5c2a5e81e21",
)

load("@com_github_google_bazel_common//:workspace_defs.bzl", "google_common_workspace_rules")
google_common_workspace_rules()

load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_dependencies")
checkstyle_dependencies()