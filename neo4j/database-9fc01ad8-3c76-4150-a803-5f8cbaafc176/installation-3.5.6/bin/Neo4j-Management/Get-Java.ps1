# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.


<#
.SYNOPSIS
Retrieves information about Java on the local machine to start Neo4j programs

.DESCRIPTION
Retrieves information about Java on the local machine to start Neo4j services and utilities, tailored to the type of Neo4j edition

.PARAMETER Neo4jServer
An object representing a valid Neo4j Server object

.PARAMETER ForServer
Retrieve the Java command line to start a Neo4j Server

.PARAMETER ForUtility
Retrieve the Java command line to start a Neo4j utility such as Neo4j Shell.

.PARAMETER StartingClass
The name of the starting class when invoking Java

.EXAMPLE
Get-Java -Neo4jServer $serverObject -ForServer

Retrieves the Java command line to start the Neo4j server for the instance in $serverObject.

.OUTPUTS
System.Collections.Hashtable

.NOTES
This function is private to the powershell module

#>
function Get-Java
{
  [CmdletBinding(SupportsShouldProcess = $false,ConfirmImpact = 'Low',DefaultParameterSetName = 'Default')]
  param(
    [Parameter(Mandatory = $true,ValueFromPipeline = $false,ParameterSetName = 'UtilityInvoke')]
    [Parameter(Mandatory = $true,ValueFromPipeline = $false,ParameterSetName = 'ServerInvoke')]
    [pscustomobject]$Neo4jServer

    ,[Parameter(Mandatory = $true,ValueFromPipeline = $false,ParameterSetName = 'ServerInvoke')]
    [switch]$ForServer

    ,[Parameter(Mandatory = $true,ValueFromPipeline = $false,ParameterSetName = 'UtilityInvoke')]
    [switch]$ForUtility

    ,[Parameter(Mandatory = $true,ValueFromPipeline = $false,ParameterSetName = 'UtilityInvoke')]
    [string]$StartingClass
  )

  begin
  {
  }

  process
  {
    $javaPath = ''
    $javaCMD = ''

    $EnvJavaHome = Get-Neo4jEnv 'JAVA_HOME'
    $EnvClassPrefix = Get-Neo4jEnv 'CLASSPATH_PREFIX'

    # Is JAVA specified in an environment variable
    if (($javaPath -eq '') -and ($EnvJavaHome -ne $null))
    {
      $javaPath = $EnvJavaHome
      # Modify the java path if a JRE install is detected
      if (Test-Path -Path "$javaPath\jre\bin\java.exe") { $javaPath = "$javaPath\jre" }
    }

    # Attempt to find Java in registry
    $regKey = 'Registry::HKLM\SOFTWARE\JavaSoft\Java Runtime Environment'
    if (($javaPath -eq '') -and (Test-Path -Path $regKey))
    {
      $regJavaVersion = ''
      try
      {
        $regJavaVersion = [string](Get-ItemProperty -Path $regKey -ErrorAction 'Stop').CurrentVersion
        if ($regJavaVersion -ne '')
        {
          $javaPath = [string](Get-ItemProperty -Path "$regKey\$regJavaVersion" -ErrorAction 'Stop').JavaHome
        }
      }
      catch
      {
        #Ignore any errors
        $javaPath = ''
      }
    }

    # Attempt to find Java in registry (32bit Java on 64bit OS)
    $regKey = 'Registry::HKLM\SOFTWARE\Wow6432Node\JavaSoft\Java Runtime Environment'
    if (($javaPath -eq '') -and (Test-Path -Path $regKey))
    {
      $regJavaVersion = ''
      try
      {
        $regJavaVersion = [string](Get-ItemProperty -Path $regKey -ErrorAction 'Stop').CurrentVersion
        if ($regJavaVersion -ne '')
        {
          $javaPath = [string](Get-ItemProperty -Path "$regKey\$regJavaVersion" -ErrorAction 'Stop').JavaHome
        }
      }
      catch
      {
        #Ignore any errors
        $javaPath = ''
      }
    }

    # Attempt to find Java in the search path
    if ($javaPath -eq '')
    {
      $javaExe = (Get-Command 'java.exe' -ErrorAction SilentlyContinue)
      if ($javaExe -ne $null)
      {
        $javaCMD = $javaExe.Path
        $javaPath = Split-Path -Path $javaCMD -Parent
      }
    }

    if ($javaPath -eq '') { Write-Error "Unable to determine the path to java.exe"; return $null }
    if ($javaCMD -eq '') { $javaCMD = "$javaPath\bin\java.exe" }
    if (-not (Test-Path -Path $javaCMD)) { Write-Error "Could not find java at $javaCMD"; return $null }

    Write-Verbose "Java detected at '$javaCMD'"

    $javaVersion = Get-JavaVersion -Path $javaCMD
    if (-not $javaVersion.isValid) { Write-Error "This instance of Java is not supported"; return $null }

    # Shell arguments for the Neo4jServer and Arbiter classes
    $ShellArgs = @()
    if ($PsCmdlet.ParameterSetName -eq 'ServerInvoke')
    {
      $serverMainClass = ''
      if ($Neo4jServer.ServerType -eq 'Enterprise') { $serverMainClass = 'com.neo4j.server.enterprise.CommercialEntryPoint' }
      if ($Neo4jServer.ServerType -eq 'Community') { $serverMainClass = 'org.neo4j.server.CommunityEntryPoint' }
      if ($Neo4jServer.DatabaseMode.ToUpper() -eq 'ARBITER') { $serverMainClass = 'org.neo4j.server.enterprise.ArbiterEntryPoint' }

      if ($serverMainClass -eq '') { Write-Error "Unable to determine the Server Main Class from the server information"; return $null }

      # Build the Java command line
      $ClassPath = "$($Neo4jServer.Home)/lib/*;$($Neo4jServer.Home)/plugins/*"
      $ShellArgs = @("-cp `"$($ClassPath)`"" `
          ,'-server' `
          ,'-Dlog4j.configuration=file:conf/log4j.properties' `
          ,'-Dneo4j.ext.udc.source=zip-powershell' `
          ,'-Dorg.neo4j.cluster.logdirectory=data/log' `
        )

      # Parse Java config settings - Heap initial size
      $option = (Get-Neo4jSetting -Name 'dbms.memory.heap.initial_size' -Neo4jServer $Neo4jServer)
      if ($option -ne $null) {
        $mem = "$($option.Value)"
        if ($mem -notmatch '[\d]+[gGmMkK]') {
          $mem += "m"
          Write-Warning @"
WARNING: dbms.memory.heap.initial_size will require a unit suffix in a
         future version of Neo4j. Please add a unit suffix to your
         configuration. Example:

         dbms.memory.heap.initial_size=512m
                                          ^
"@
        }
        $ShellArgs += "-Xms$mem"
      }

      # Parse Java config settings - Heap max size
      $option = (Get-Neo4jSetting -Name 'dbms.memory.heap.max_size' -Neo4jServer $Neo4jServer)
      if ($option -ne $null) {
        $mem = "$($option.Value)"
        if ($mem -notmatch '[\d]+[gGmMkK]') {
          $mem += "m"
          Write-Warning @"
WARNING: dbms.memory.heap.max_size will require a unit suffix in a
         future version of Neo4j. Please add a unit suffix to your
         configuration. Example:

         dbms.memory.heap.max_size=512m
                                      ^
"@
        }
        $ShellArgs += "-Xmx$mem"
      }

      # Parse Java config settings - Explicit
      $option = (Get-Neo4jSetting -Name 'dbms.jvm.additional' -Neo4jServer $Neo4jServer)
      if ($option -ne $null) { $ShellArgs += $option.value }

      # Parse Java config settings - GC
      $option = (Get-Neo4jSetting -Name 'dbms.logs.gc.enabled' -Neo4jServer $Neo4jServer)
      if (($option -ne $null) -and ($option.value.ToLower() -eq 'true')) {
        if ($javaVersion.isJava8) {
          # JAVA 8 GC logs configuration
          $ShellArgs += "-Xloggc:`"$($Neo4jServer.LogDir)/gc.log`""

          $option = (Get-Neo4jSetting -Name 'dbms.logs.gc.options' -Neo4jServer $Neo4jServer)
          if ($option -eq $null) {
            $ShellArgs += @('-XX:+PrintGCDetails',
              '-XX:+PrintGCDateStamps',
              '-XX:+PrintGCApplicationStoppedTime',
              '-XX:+PrintPromotionFailure',
              '-XX:+PrintTenuringDistribution',
              '-XX:+UseGCLogFileRotation')
          } else {
            # The GC options _should_ be space delimited
            $ShellArgs += ($option.value -split ' ')
          }

          $option = (Get-Neo4jSetting -Name 'dbms.logs.gc.rotation.size' -Neo4jServer $Neo4jServer)
          if ($option -ne $null) {
            $ShellArgs += "-XX:GCLogFileSize=$( $option.value )"
          } else {
            $ShellArgs += "-XX:GCLogFileSize=20m"
          }

          $option = (Get-Neo4jSetting -Name 'dbms.logs.gc.rotation.keep_number' -Neo4jServer $Neo4jServer)
          if ($option -ne $null) {
            $ShellArgs += "-XX:NumberOfGCLogFiles=$( $option.value )"
          } else {
            $ShellArgs += "-XX:NumberOfGCLogFiles=5"
          }
        } else {
          # JAVA 9 and newer GC logs configuration
          $option = (Get-Neo4jSetting -Name 'dbms.logs.gc.options' -Neo4jServer $Neo4jServer)
          if ($option -ne $null) {
            $gcOptions = $option.value
          } else {
            $gcOptions = '-Xlog:gc*,safepoint,age*=trace'
          }
          # GC file name should be escaped on Windows because of ':' usage as part of absolute name
          $gcFile = "\`"" + "$($Neo4jServer.LogDir)/gc.log" + "\`""
          $gcOptions += ":file=$( $gcFile )::"

          $option = (Get-Neo4jSetting -Name 'dbms.logs.gc.rotation.keep_number' -Neo4jServer $Neo4jServer)
          if ($option -ne $null) {
            $gcOptions += "filecount=$( $option.value )"
          } else {
            $gcOptions += "filecount=5"
          }

          $option = (Get-Neo4jSetting -Name 'dbms.logs.gc.rotation.size' -Neo4jServer $Neo4jServer)
          if ($option -ne $null) {
            $gcOptions += ",filesize=$( $option.value )"
          } else {
            $gcOptions += ",filesize=20m"
          }
          $ShellArgs += $gcOptions
        }
      }
      $ShellArgs += @("-Dfile.encoding=UTF-8",
        $serverMainClass,
        "--config-dir=`"$($Neo4jServer.ConfDir)`"",
        "--home-dir=`"$($Neo4jServer.Home)`"")
    }

    # Shell arguments for the utility classes e.g. Import, Shell
    if ($PsCmdlet.ParameterSetName -eq 'UtilityInvoke')
    {
      # Generate the commandline args
      $ClassPath = "$($Neo4jServer.Home)/lib/*;$($Neo4jServer.Home)/bin/*"
      # Augment with tools.jar if found
      if (Test-Path -Path "$EnvJavaHome\lib\tools.jar") { $ClassPath += ";$EnvJavaHome\lib\tools.jar" }

      $ShellArgs = @()
      $ShellArgs += @("-XX:+UseParallelGC",
        "-classpath `"$($EnvClassPrefix);$ClassPath`"",
        "-Dbasedir=`"$($Neo4jServer.Home)`"",`
           '-Dfile.encoding=UTF-8')

      # Determine user configured heap size.
      $HeapSize = Get-Neo4jEnv 'HEAP_SIZE'
      if ($HeapSize -ne $null) {
        $ShellArgs += "-Xmx$HeapSize"
        $ShellArgs += "-Xms$HeapSize"
      }

      # Add the starting class
      $ShellArgs += @($StartingClass)
    }

    Write-Output @{ 'java' = $javaCMD; 'args' = $ShellArgs }
  }

  end
  {
  }
}

# SIG # Begin signature block
# MIId1gYJKoZIhvcNAQcCoIIdxzCCHcMCAQExDzANBglghkgBZQMEAgEFADB5Bgor
# BgEEAYI3AgEEoGswaTA0BgorBgEEAYI3AgEeMCYCAwEAAAQQH8w7YFlLCE63JNLG
# KX7zUQIBAAIBAAIBAAIBAAIBADAxMA0GCWCGSAFlAwQCAQUABCBRRnq7Lr2I8P7B
# EMSFoWqRt0Di4cA2c+VWHh5+tYi4kaCCGEYwggPFMIICraADAgECAgEAMA0GCSqG
# SIb3DQEBCwUAMIGDMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHQXJpem9uYTETMBEG
# A1UEBxMKU2NvdHRzZGFsZTEaMBgGA1UEChMRR29EYWRkeS5jb20sIEluYy4xMTAv
# BgNVBAMTKEdvIERhZGR5IFJvb3QgQ2VydGlmaWNhdGUgQXV0aG9yaXR5IC0gRzIw
# HhcNMDkwOTAxMDAwMDAwWhcNMzcxMjMxMjM1OTU5WjCBgzELMAkGA1UEBhMCVVMx
# EDAOBgNVBAgTB0FyaXpvbmExEzARBgNVBAcTClNjb3R0c2RhbGUxGjAYBgNVBAoT
# EUdvRGFkZHkuY29tLCBJbmMuMTEwLwYDVQQDEyhHbyBEYWRkeSBSb290IENlcnRp
# ZmljYXRlIEF1dGhvcml0eSAtIEcyMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIB
# CgKCAQEAv3FiCPH6WTT3G8kYo/eASVjpIoMTpsUgQwE7hPHmhUmfJ+r2hBtOoLTb
# cJjHMgGxBT4HTu70+k8vWTAi56sZVmvigAf88xZ1gDlRe+X5NbZ0TqmNghPktj+p
# A4P6or6KFWp/3gvDthkUBcrqw6gElDtGfDIN8wBmIsiNaW02jBEYt9OyHGC0OPoC
# jM7T3UYH3go+6118yHz7sCtTpJJiaVElBWEaRIGMLKlDliPfrDqBmg4pxRyp6V0e
# tp6eMAo5zvGIgPtLXcwy7IViQyU0AlYnAZG0O3AqP26x6JyIAX2f1PnbU21gnb8s
# 51iruF9G/M7EGwM8CetJMVxpRrPgRwIDAQABo0IwQDAPBgNVHRMBAf8EBTADAQH/
# MA4GA1UdDwEB/wQEAwIBBjAdBgNVHQ4EFgQUOpqFBxBnKLbv9r0FQW4gwZTaD94w
# DQYJKoZIhvcNAQELBQADggEBAJnbXXnV+ZdZZwNh8X47BjF1LaEgjk9lh7T3ppy8
# 2Okv0Nta7s90jHO0OELaBXv4AnW4/aWx1672194Ty1MQfopG0Zf6ty4rEauQsCeA
# +eifWuk3n6vk32yzhRedPdkkT3mRNdZfBOuAg6uaAi21EPTYkMcEc0DtciWgqZ/s
# nqtoEplXxo8SOgmkvUT9BhU3wZvkMqPtOOjYZPMsfhT8Auqfzf8HaBfbIpA4LXqN
# 0VTxaeNfM8p6PXsK48p/Xznl4nW6xXYYM84s8C9Mrfex585PqMSbSlQGxX991QgP
# 4hz+fhe4rF721BayQwkMTfana7SZhGXKeoji4kS+XPfqHPUwggTQMIIDuKADAgEC
# AgEHMA0GCSqGSIb3DQEBCwUAMIGDMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHQXJp
# em9uYTETMBEGA1UEBxMKU2NvdHRzZGFsZTEaMBgGA1UEChMRR29EYWRkeS5jb20s
# IEluYy4xMTAvBgNVBAMTKEdvIERhZGR5IFJvb3QgQ2VydGlmaWNhdGUgQXV0aG9y
# aXR5IC0gRzIwHhcNMTEwNTAzMDcwMDAwWhcNMzEwNTAzMDcwMDAwWjCBtDELMAkG
# A1UEBhMCVVMxEDAOBgNVBAgTB0FyaXpvbmExEzARBgNVBAcTClNjb3R0c2RhbGUx
# GjAYBgNVBAoTEUdvRGFkZHkuY29tLCBJbmMuMS0wKwYDVQQLEyRodHRwOi8vY2Vy
# dHMuZ29kYWRkeS5jb20vcmVwb3NpdG9yeS8xMzAxBgNVBAMTKkdvIERhZGR5IFNl
# Y3VyZSBDZXJ0aWZpY2F0ZSBBdXRob3JpdHkgLSBHMjCCASIwDQYJKoZIhvcNAQEB
# BQADggEPADCCAQoCggEBALngyxDUr3a91JNi6zBkuIEIbMME2WIXji//PmXPj85i
# 5jxSHNoWRUtVq3hrY4NikM4PaWyZyBoUi0zMRTPqiNyeo68r/oBhnXlXxM8u9D8w
# PF1H/JoWvMM3lkFRjhFLVPgovtCMvvAwOB7zsCb4Zkdjbd5xJkePOEdT0UYdtOPc
# AOpFrL28cdmqbwDb280wOnlPX0xH+B3vW8LEnWA7sbJDkdikM07qs9YnT60liqXG
# 9NXQpq50BWRXiLVEVdQtKjo++Li96TIKApRkxBY6UPFKrud5M68MIAd/6N8EOcJp
# AmxjUvp3wRvIdIfIuZMYUFQ1S2lOvDvTSS4f3MHSUvsCAwEAAaOCARowggEWMA8G
# A1UdEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEGMB0GA1UdDgQWBBRAwr0njsw0
# gzCiM9f7bLPwtCyAzjAfBgNVHSMEGDAWgBQ6moUHEGcotu/2vQVBbiDBlNoP3jA0
# BggrBgEFBQcBAQQoMCYwJAYIKwYBBQUHMAGGGGh0dHA6Ly9vY3NwLmdvZGFkZHku
# Y29tLzA1BgNVHR8ELjAsMCqgKKAmhiRodHRwOi8vY3JsLmdvZGFkZHkuY29tL2dk
# cm9vdC1nMi5jcmwwRgYDVR0gBD8wPTA7BgRVHSAAMDMwMQYIKwYBBQUHAgEWJWh0
# dHBzOi8vY2VydHMuZ29kYWRkeS5jb20vcmVwb3NpdG9yeS8wDQYJKoZIhvcNAQEL
# BQADggEBAAh+bJMQyDi4lqmQS/+hX08E72w+nIgGyVCPpnP3VzEbvrzkL9v4utNb
# 4LTn5nliDgyi12pjczG19ahIpDsILaJdkNe0fCVPEVYwxLZEnXssneVe5u8MYaq/
# 5Cob7oSeuIN9wUPORKcTcA2RH/TIE62DYNnYcqhzJB61rCIOyheJYlhEG6uJJQEA
# D83EG2LbUbTTD1Eqm/S8c/x2zjakzdnYLOqum/UqspDRTXUYij+KQZAjfVtL/qQD
# WJtGssNgYIP4fVBBzsKhkMO77wIv0hVU7kQV2Qqup4oz7bEtdjYm3ATrn/dhHxXc
# h2/uRpYoraEmfQoJpy4Eo428+LwEMAEwggUAMIID6KADAgECAgEHMA0GCSqGSIb3
# DQEBCwUAMIGPMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHQXJpem9uYTETMBEGA1UE
# BxMKU2NvdHRzZGFsZTElMCMGA1UEChMcU3RhcmZpZWxkIFRlY2hub2xvZ2llcywg
# SW5jLjEyMDAGA1UEAxMpU3RhcmZpZWxkIFJvb3QgQ2VydGlmaWNhdGUgQXV0aG9y
# aXR5IC0gRzIwHhcNMTEwNTAzMDcwMDAwWhcNMzEwNTAzMDcwMDAwWjCBxjELMAkG
# A1UEBhMCVVMxEDAOBgNVBAgTB0FyaXpvbmExEzARBgNVBAcTClNjb3R0c2RhbGUx
# JTAjBgNVBAoTHFN0YXJmaWVsZCBUZWNobm9sb2dpZXMsIEluYy4xMzAxBgNVBAsT
# Kmh0dHA6Ly9jZXJ0cy5zdGFyZmllbGR0ZWNoLmNvbS9yZXBvc2l0b3J5LzE0MDIG
# A1UEAxMrU3RhcmZpZWxkIFNlY3VyZSBDZXJ0aWZpY2F0ZSBBdXRob3JpdHkgLSBH
# MjCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAOWQZkvs+UZxqSCDvuls
# v0rJSGmBdU5tJPbLFxP4sHFZhHprK4WkNLUW5cvM6UFwLKQu1voyfeGo3pQQrDHB
# wNhq/1knq3bW/At0a7inrj/EVPS0MUTdk1aMpExem4nLJIOb4ld9t9gSH8mFbfTR
# gPFQm4eu1AsQBfsnuihtF+kO1k25OVUG/wokBX4vxh1ybNSLKYxXfdrZ62Ya00+n
# 339SxDDFpckOAsVTv3c4aAYkw2bIN34wHkVxIzX/kNgqnY3nsJJNPH8qCpPczRZG
# ZfdghIt2S5EncxSS4OrujxbqjQ4+dhe/fYmAgERD5y3gQwl12jborduJOvVdEo4j
# BIMCAwEAAaOCASwwggEoMA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEG
# MB0GA1UdDgQWBBQlRYFoUCY4PTstLL7Natm2PbNmYzAfBgNVHSMEGDAWgBR8DDIf
# p9kwf8R9aKNiqKHOqwdbJzA6BggrBgEFBQcBAQQuMCwwKgYIKwYBBQUHMAGGHmh0
# dHA6Ly9vY3NwLnN0YXJmaWVsZHRlY2guY29tLzA7BgNVHR8ENDAyMDCgLqAshipo
# dHRwOi8vY3JsLnN0YXJmaWVsZHRlY2guY29tL3Nmcm9vdC1nMi5jcmwwTAYDVR0g
# BEUwQzBBBgRVHSAAMDkwNwYIKwYBBQUHAgEWK2h0dHBzOi8vY2VydHMuc3RhcmZp
# ZWxkdGVjaC5jb20vcmVwb3NpdG9yeS8wDQYJKoZIhvcNAQELBQADggEBAFZlyv7z
# Pwqok4sYx95DaRM0IL5OX3ioa5zbak1B28ET7NwxACJe9wCeDOA0ZTT5sTpOSMgS
# gYhcWz4IU3r3GmTfuFBhzFNRQClLwvSuOl/kyq0mzE5hQ+X9V6Y3cM5DK7CUw5Lp
# 4V+qEEm3aeTg0B9kpCvNH2+g+IQkGM55PamRv1QYE4mZVBENVcUmC3lPWhxu+WPb
# FICkB6v6sqW5iN2R/mU7pKN5volN4dCw9MgXDAqWFHwJt2zhwthV1BigqkFpcCSj
# ue/pWtw+65RK8LfeXw52+vv7aQNFQFDucgykEoaBzRPRTsQ8yk4N0ibxALe0pqLh
# bnqB/TCseh/HWXswggUhMIIECaADAgECAgkAhHYYKGL3whowDQYJKoZIhvcNAQEL
# BQAwgbQxCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdBcml6b25hMRMwEQYDVQQHEwpT
# Y290dHNkYWxlMRowGAYDVQQKExFHb0RhZGR5LmNvbSwgSW5jLjEtMCsGA1UECxMk
# aHR0cDovL2NlcnRzLmdvZGFkZHkuY29tL3JlcG9zaXRvcnkvMTMwMQYDVQQDEypH
# byBEYWRkeSBTZWN1cmUgQ2VydGlmaWNhdGUgQXV0aG9yaXR5IC0gRzIwHhcNMTcx
# MTA3MTkzNzAzWhcNMjAxMTA3MTkzNzAzWjBiMQswCQYDVQQGEwJVUzETMBEGA1UE
# CBMKQ2FsaWZvcm5pYTESMBAGA1UEBxMJU2FuIE1hdGVvMRQwEgYDVQQKEwtOZW80
# aiwgSW5jLjEUMBIGA1UEAxMLTmVvNGosIEluYy4wggEiMA0GCSqGSIb3DQEBAQUA
# A4IBDwAwggEKAoIBAQDSoPiG1pU1Lvqo+aZsFTrUwaV1sDWVBtfWzSnDKB3bUJeC
# 7DhekXtt1FORi3PB4YAC/CSMGgwoBHuqgGuRaJbHjRlmYaZZdKVsgvmDwfEvv16j
# zoyUR8TMTTjCemIDAHwArEadkffpsgnFpQ6KG6+gag/39FXyM2rGmFaqSGkqjVRN
# u4zN5GQu8+CUvRuZO2zEuKdA4wv9ZlmWbV3bpCGIN3Zl4p39Fatz3KYNi4g8lFXh
# B8tJfBToRuqxLZpcuyrXG3PeLa6DNoYOJ3j49DJOEw8Wj9cnqvAaI3CNE2klZ7RS
# cE47YUh7rVpl/ykp9ohgZDtvhAA5RYI5KCnc+oXHAgMBAAGjggGFMIIBgTAMBgNV
# HRMBAf8EAjAAMBMGA1UdJQQMMAoGCCsGAQUFBwMDMA4GA1UdDwEB/wQEAwIHgDA1
# BgNVHR8ELjAsMCqgKKAmhiRodHRwOi8vY3JsLmdvZGFkZHkuY29tL2dkaWcyczUt
# My5jcmwwXQYDVR0gBFYwVDBIBgtghkgBhv1tAQcXAjA5MDcGCCsGAQUFBwIBFito
# dHRwOi8vY2VydGlmaWNhdGVzLmdvZGFkZHkuY29tL3JlcG9zaXRvcnkvMAgGBmeB
# DAEEATB2BggrBgEFBQcBAQRqMGgwJAYIKwYBBQUHMAGGGGh0dHA6Ly9vY3NwLmdv
# ZGFkZHkuY29tLzBABggrBgEFBQcwAoY0aHR0cDovL2NlcnRpZmljYXRlcy5nb2Rh
# ZGR5LmNvbS9yZXBvc2l0b3J5L2dkaWcyLmNydDAfBgNVHSMEGDAWgBRAwr0njsw0
# gzCiM9f7bLPwtCyAzjAdBgNVHQ4EFgQUvj4gytCNJMDPx3lWv0klX6YK41IwDQYJ
# KoZIhvcNAQELBQADggEBABzaEnMJczETlZUdZE36x84eQS2AmumczZzTMbZ4IhJw
# xF8vVz2+Q+0BcR5uwAXa+s167yqIZsxAub3nu8GzYAF7D7wHDC1H1JNkgfnZf1w2
# WWGL6jkbr5RGrLlU2xE8o03iuFglU4QQl9ouXXBLAsLo/q+pMrPs+EO+g3DwXGFt
# jAKzkrMzJD5Ia2kVSC2aAXrffwRqMpbKVxkf0TQadMGLa6dVybYH7qBfDZ+u8P2K
# Y0qQyQYY63WoVk7TIq1VkbmRXtcvm3/plWPUNTPPEy0DfnjndA2UByib6/iqdnSZ
# 7MYit31rmSsRAS3Wil/qqOGlVfYrSm2s64ryPMOacAkwggV8MIIEZKADAgECAghX
# 62vcgKuJTDANBgkqhkiG9w0BAQsFADCBxjELMAkGA1UEBhMCVVMxEDAOBgNVBAgT
# B0FyaXpvbmExEzARBgNVBAcTClNjb3R0c2RhbGUxJTAjBgNVBAoTHFN0YXJmaWVs
# ZCBUZWNobm9sb2dpZXMsIEluYy4xMzAxBgNVBAsTKmh0dHA6Ly9jZXJ0cy5zdGFy
# ZmllbGR0ZWNoLmNvbS9yZXBvc2l0b3J5LzE0MDIGA1UEAxMrU3RhcmZpZWxkIFNl
# Y3VyZSBDZXJ0aWZpY2F0ZSBBdXRob3JpdHkgLSBHMjAeFw0xOTA0MTUwNzAwMDBa
# Fw0yNDA0MTUwNzAwMDBaMIGHMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHQXJpem9u
# YTETMBEGA1UEBxMKU2NvdHRzZGFsZTEkMCIGA1UEChMbU3RhcmZpZWxkIFRlY2hu
# b2xvZ2llcywgTExDMSswKQYDVQQDEyJTdGFyZmllbGQgVGltZXN0YW1wIEF1dGhv
# cml0eSAtIEcyMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAzq8g420K
# 52oSzwX8q/EV5sVYhdhJzrc5/FQbiQOrax0PZYMoBKenJWo9sRQkwqkrxoTLzozz
# PqU/LG2H5knkw625cSMHB74FGHtPHjEju4tfnQFDI+XPzRGhvSPMrzPvg5zGGVT/
# Fku5z+AjU6LPPNFWR6vxVsz1Zq0y6cSLL0iJojP01SR6VtfNpzDMCu7p8OtpttH/
# GG/ohj3DMurv0NCrerzojzmWEr5oG8n2qtVAh2F/KmbzNmQ2wHNzGVFGm9YB1iVd
# G9M+V7zAs3HYbnSJKFi+k14UFgEYaiVnhxRCS+KREy8nEPaF6UII16Fxm3gJhJhO
# u3z25YPkk74PYQIDAQABo4IBqTCCAaUwDAYDVR0TAQH/BAIwADAOBgNVHQ8BAf8E
# BAMCBsAwFgYDVR0lAQH/BAwwCgYIKwYBBQUHAwgwHQYDVR0OBBYEFHqrqS4tyGzj
# JXj2RtbFmwi6fyrvMB8GA1UdIwQYMBaAFCVFgWhQJjg9Oy0svs1q2bY9s2ZjMIGE
# BggrBgEFBQcBAQR4MHYwKgYIKwYBBQUHMAGGHmh0dHA6Ly9vY3NwLnN0YXJmaWVs
# ZHRlY2guY29tLzBIBggrBgEFBQcwAoY8aHR0cDovL2NybC5zdGFyZmllbGR0ZWNo
# LmNvbS9yZXBvc2l0b3J5L3NmX2lzc3VpbmdfY2EtZzIuY3J0MFQGA1UdHwRNMEsw
# SaBHoEWGQ2h0dHA6Ly9jcmwuc3RhcmZpZWxkdGVjaC5jb20vcmVwb3NpdG9yeS9t
# YXN0ZXJzdGFyZmllbGQyaXNzdWluZy5jcmwwUAYDVR0gBEkwRzBFBgtghkgBhv1u
# AQcXAjA2MDQGCCsGAQUFBwIBFihodHRwOi8vY3JsLnN0YXJmaWVsZHRlY2guY29t
# L3JlcG9zaXRvcnkvMA0GCSqGSIb3DQEBCwUAA4IBAQA4Pf90cACyj2vf+vYBwzlD
# 1DQg6ZZ8qQy4LvOSvlBz1mcTb0cR9FTUoyY40Y2KELo/VMY97rSUE3XeYiuCDdQS
# 9LCb2JlVF4m68aEFryeLqge7zvn039iYOw2icq5we8YuRd0JgCX338GSXPHHFdXX
# wz2pq/99uzoFczv86U3+usaG9tfYPg+hda1BIzCDPYS9YcqaqUgMAswM//VM4rX5
# TKfKiaWvfDSumRS4zAsQVHxeiov1ucfjnozIsPguFbO6wZDlfl/+kOMPnsmxD3HA
# cAb0LSVrgjdk6HR12SQKtOVhGtbXWQMUPpmK9Txqygmj17zMMdYlHwNwo9ZCUprM
# MYIE5jCCBOICAQEwgcIwgbQxCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdBcml6b25h
# MRMwEQYDVQQHEwpTY290dHNkYWxlMRowGAYDVQQKExFHb0RhZGR5LmNvbSwgSW5j
# LjEtMCsGA1UECxMkaHR0cDovL2NlcnRzLmdvZGFkZHkuY29tL3JlcG9zaXRvcnkv
# MTMwMQYDVQQDEypHbyBEYWRkeSBTZWN1cmUgQ2VydGlmaWNhdGUgQXV0aG9yaXR5
# IC0gRzICCQCEdhgoYvfCGjANBglghkgBZQMEAgEFAKCBhDAYBgorBgEEAYI3AgEM
# MQowCKACgAChAoAAMBkGCSqGSIb3DQEJAzEMBgorBgEEAYI3AgEEMBwGCisGAQQB
# gjcCAQsxDjAMBgorBgEEAYI3AgEVMC8GCSqGSIb3DQEJBDEiBCA10oxSNlz+b3Sw
# mWboht0oUKyV9DSpeiX7Q891Wbuw+DANBgkqhkiG9w0BAQEFAASCAQB/TGv4gBOm
# K6IKnoBsIaDDQfxCJzK1z6cYsa61RqUFwxSG+VczkznFq80YZvQ3aOPqLUhouyfP
# KUmunNBck75xsNwpfQYohFleSci0limrZTfW+/L3DRXkRxcLNuNvBfEOerUG4qCi
# nf0YKIPMTj2vBolVoo+oP7jll8oAuE/nUinmp/NMpca+FMqkbp5oIxnqXrHzryQ8
# LVMnzq/ALIz//tmZIituKhnbIzRMYfZz6G46NRmOC3nHqKMx0kNVEdX8Xv6+D1CB
# i4igt6ML6Ek8FYVrUjz3xk6GsbMA7t6/iJ57mrtnjnM/LptxFI1BNfPAQ5Zo5uqT
# 0pK9veZ9mXyboYICbTCCAmkGCSqGSIb3DQEJBjGCAlowggJWAgEBMIHTMIHGMQsw
# CQYDVQQGEwJVUzEQMA4GA1UECBMHQXJpem9uYTETMBEGA1UEBxMKU2NvdHRzZGFs
# ZTElMCMGA1UEChMcU3RhcmZpZWxkIFRlY2hub2xvZ2llcywgSW5jLjEzMDEGA1UE
# CxMqaHR0cDovL2NlcnRzLnN0YXJmaWVsZHRlY2guY29tL3JlcG9zaXRvcnkvMTQw
# MgYDVQQDEytTdGFyZmllbGQgU2VjdXJlIENlcnRpZmljYXRlIEF1dGhvcml0eSAt
# IEcyAghX62vcgKuJTDAJBgUrDgMCGgUAoF0wGAYJKoZIhvcNAQkDMQsGCSqGSIb3
# DQEHATAcBgkqhkiG9w0BCQUxDxcNMTkwNTIzMDY0MzM3WjAjBgkqhkiG9w0BCQQx
# FgQUYibRw4jGuILDYj0JZRuS5IqGk+8wDQYJKoZIhvcNAQEBBQAEggEAsAtnxROz
# sy2EKC9KnDaal0fRBs8W5iJaOUdOhxWzqrIAbnKgPjdCLFQVUlJ2duwQTp+uqE6w
# +cU9llW7yH1jSUaB5CW19oUv/ZGWo9iiXDH2hqPjDMCKLVw578tvtMeqBLfYL6l/
# 2kU+fFQddgf0U3CyG82xheXc7RLmF18mAo0fy52y+CqTYoAS+8VWhBCWXO2AEDoR
# PrroGBA361BFjmDgHv4R5UCeaY0uYks1pMWXMZs/Tbvu+ALpGkq1+iPrtape3QKm
# 9/QDjKGL2QGVZk2Mi+gWLmu0tySYC8utLGLoxiNg67HmdpmIHOvdU70nnNOdfdxE
# XUkuL13zLL2PRw==
# SIG # End signature block
