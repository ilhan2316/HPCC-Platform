<?xml version="1.0" encoding="UTF-8"?>

<!--
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xml:space="default"
xmlns:set="http://exslt.org/sets">
    <xsl:template name="LogSourceMap">
        <xsl:param name="agentNode"/>
        <LogSourceMap>
            <xsl:for-each select="$agentNode/LogSourceMap/LogSource">
                <xsl:if test="string(current()/@name) = ''">
                    <xsl:message terminate="yes">LogSource name is undefined!</xsl:message>
                </xsl:if>
                <xsl:if test="string(current()/@mapToDB) = ''">
                    <xsl:message terminate="yes">LogSource mapToDB is undefined for <xsl:value-of select="current()/@name"/>!</xsl:message>
                </xsl:if>
                <xsl:if test="string(current()/@mapToLogGroup) = ''">
                    <xsl:message terminate="yes">LogSource mapToLogGroup is undefined for <xsl:value-of select="current()/@name"/>!</xsl:message>
                </xsl:if>
                <LogSource name="{current()/@name}" maptodb="{current()/@mapToDB}" maptologgroup="{current()/@mapToLogGroup}"/>
            </xsl:for-each>
        </LogSourceMap>
    </xsl:template>

    <xsl:template name="LogGroup">
        <xsl:param name="agentNode"/>
        <xsl:for-each select="$agentNode/LogGroup">
            <LogGroup name="{current()/@name}">
                <xsl:for-each select="current()/Fieldmap">
                    <Fieldmap table="{current()/@table}">
                        <xsl:for-each select="current()/Field">
                            <xsl:if test="string(current()/@name) = ''">
                                <xsl:message terminate="yes">Field name is undefined!</xsl:message>
                            </xsl:if>
                            <xsl:if test="string(current()/@mapTo) = ''">
                                <xsl:message terminate="yes">Field mapTo is undefined for <xsl:value-of select="current()/@name"/>!</xsl:message>
                            </xsl:if>
                            <xsl:if test="string(current()/@type) = ''">
                                <xsl:message terminate="yes">Field type is undefined for <xsl:value-of select="current()/@name"/>!</xsl:message>
                            </xsl:if>
                            <xsl:choose>
                                <xsl:when test="string(current()/@default) = ''">
                                    <Field name="{current()/@name}" mapto="{current()/@mapTo}" type="{current()/@type}"/>
                                </xsl:when>
                                <xsl:otherwise>
                                    <Field name="{current()/@name}" mapto="{current()/@mapTo}" type="{current()/@type}" default="{current()/@default}"/>
                                </xsl:otherwise>
                            </xsl:choose>
                        </xsl:for-each>
                    </Fieldmap>
                </xsl:for-each>
            </LogGroup>
        </xsl:for-each>
    </xsl:template>

    <xsl:template name="CassandraLoggingAgent">
        <xsl:for-each select="CassandraLoggingAgents">
            <xsl:choose>
                <xsl:when test="string(@CassandraLoggingAgent) = ''">
                    <xsl:message terminate="yes">Cassandra Logging Agent name is undefined!</xsl:message>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:variable name="agentName" select="@CassandraLoggingAgent"/>
                    <xsl:variable name="agentNode" select="/Environment/Software/CassandraLoggingAgent[@name=$agentName]"/>
                    <xsl:if test="not($agentNode)">
                        <xsl:message terminate="yes"><xsl:value-of select="$agentName"/> does not match with any Cassandra Logging Agent inside this environment!</xsl:message>
                    </xsl:if>
                    <xsl:if test="string($agentNode/@serverIP) = ''">
                        <xsl:message terminate="yes">Cassandra server network address is undefined for <xsl:value-of select="$agentName"/>!</xsl:message>
                    </xsl:if>
                    <LogAgent name="{$agentName}" type="LogAgent" services="GetTransactionSeed,UpdateLog,GetTransactionID" plugin="cassandralogagent">
                        <Cassandra server="{$agentNode/@serverIP}" dbUser="{$agentNode/@userName}" dbPassWord="{$agentNode/@userPassword}" dbName="{$agentNode/@ksName}"/>
                        <xsl:if test="string($agentNode/@FailSafe) != ''">
                            <FailSafe><xsl:value-of select="$agentNode/@FailSafe"/></FailSafe>
                        </xsl:if>
                        <xsl:if test="string($agentNode/@FailSafeLogsDir) != ''">
                            <FailSafeLogsDir><xsl:value-of select="$agentNode/@FailSafeLogsDir"/></FailSafeLogsDir>
                        </xsl:if>
                        <xsl:if test="string($agentNode/@MaxLogQueueLength) != ''">
                            <MaxLogQueueLength><xsl:value-of select="$agentNode/@MaxLogQueueLength"/></MaxLogQueueLength>
                        </xsl:if>
                        <xsl:if test="string($agentNode/@MaxTriesGTS) != ''">
                            <MaxTriesGTS><xsl:value-of select="$agentNode/@MaxTriesGTS"/></MaxTriesGTS>
                        </xsl:if>
                        <xsl:if test="string($agentNode/@MaxTriesRS) != ''">
                            <MaxTriesRS><xsl:value-of select="$agentNode/@MaxTriesRS"/></MaxTriesRS>
                        </xsl:if>
                        <xsl:if test="string($agentNode/@QueueSizeSignal) != ''">
                            <QueueSizeSignal><xsl:value-of select="$agentNode/@QueueSizeSignal"/></QueueSizeSignal>
                        </xsl:if>
                        <xsl:if test="string($agentNode/@defaultTransaction) != ''">
                            <defaultTransaction><xsl:value-of select="$agentNode/@defaultTransaction"/></defaultTransaction>
                        </xsl:if>
                        <xsl:if test="string($agentNode/@loggingTransaction) != ''">
                            <LoggingTransaction><xsl:value-of select="$agentNode/@loggingTransaction"/></LoggingTransaction>
                        </xsl:if>
                        <xsl:if test="string($agentNode/@logSourcePath) != ''">
                            <LogSourcePath><xsl:value-of select="$agentNode/@logSourcePath"/></LogSourcePath>
                        </xsl:if>

                        <xsl:call-template name="LogSourceMap">
                            <xsl:with-param name="agentNode" select="$agentNode"/>
                        </xsl:call-template>
                        <xsl:call-template name="LogGroup">
                            <xsl:with-param name="agentNode" select="$agentNode"/>
                        </xsl:call-template>
                    </LogAgent>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:for-each>
    </xsl:template>

</xsl:stylesheet>
