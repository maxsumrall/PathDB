#!/usr/bin/env bash
#
# Copyright (C) 2015-2017 - All rights reserved.
# This file is part of the pathdb project which is released under the GPLv3 license.
# See file LICENSE.txt or go to http://www.gnu.org/licenses/gpl.txt for full license details.
# You may use, distribute and modify this code under the terms of the GPLv3 license.
#

if [ ! -z "$TRAVIS_TAG" ]
then
    echo "on a tag -> set pom.xml <version> to $TRAVIS_TAG"
    mvn --settings .travis/settings.xml org.codehaus.mojo:versions-maven-plugin:2.1:set -DnewVersion=$TRAVIS_TAG 1>/dev/null 2>/dev/null
else
    echo "not on a tag -> keep snapshot version in pom.xml"
fi
mvn clean deploy --settings .travis/settings.xml -DskipTests=true -DperformRelease=true -B -U
