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


####################
# Load Build Tools #
####################

# Load additional build tools, such bazel-deps and unused-deps
load("//dependencies/tools:dependencies.bzl", "tools_dependencies")
tools_dependencies()

load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_dependencies")
checkstyle_dependencies()

#####################################
# Load Java dependencies from Maven #
#####################################

load("//dependencies/maven:dependencies.bzl", "maven_dependencies")
maven_dependencies()


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


########################################
#     Load Deployment Dependencies     #
########################################

git_repository(
    name="graknlabs_bazel_distribution",
    remote="https://github.com/graknlabs/bazel-distribution",
    commit="796cea2531404509a3298af65ab562b1929c3eb6"
)

load("@graknlabs_bazel_distribution//github:dependencies.bzl", "github_dependencies_for_deployment")
github_dependencies_for_deployment()

git_repository(
    name="com_github_google_bazel_common",
    remote="https://github.com/graknlabs/bazel-common",
    commit="550f0490798a4e4b6c5ff8cac3b6f5c2a5e81e21",
)

load("@com_github_google_bazel_common//:workspace_defs.bzl", "google_common_workspace_rules")
google_common_workspace_rules()